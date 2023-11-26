/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    /**
     * 一次最多删除10个文件
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 文件队列的存储路径
     */
    private final String storePath;

    /**
     * 每个mappedFile文件的大小
     * CommitLog：MessageStoreConfig.mappedFileSizeCommitLog，默认1G
     * ConsumeQueue：MessageStoreConfig.mappedFileSizeConsumeQueue，默认30w个ConsumeQueue存储单元（600w字节）
     * ConsumeQueueExt：MessageStoreConfig.mappedFileSizeConsumeQueueExt，默认48MB
     */
    protected final int mappedFileSize;

    /**
     * 写时复制
     * 当前MappedFileQueue的mappedFiles
     */
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 分配mappedFile的线程服务
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 刷盘最新位置：已flush（刷盘）到的位置(某一个mappedFile中的一个位置（偏移地址）)
     *  CommitLog的整体已刷盘物理偏移量
     */
    protected long flushedWhere = 0;
    /**
     * 提交的最新位置：已commit到的位置(某一个mappedFile中的一个位置（偏移地址）)
     */
    private long committedWhere = 0;

    /**
     * 当前目录下最后一条数据的存储时间
     */
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * MappedFileQueue#checkSelf()
     * 检查映射文件队列中的文件大小是否正确。
     * 检查mappedFiles中，除去最后一个文件，如果两个文件之间偏移量差（每一个mappedFile的大小）不等于文件大小（mappedFileSize），则打印日志
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * MappedFileQueue#truncateDirtyFiles(long)
     * 截断无效文件:处理offset以上的MappedFile中认为是dirty的部分
     * MappedFile包含了offset的去掉offset后续部分
     * MappedFile超过了offset的直接删掉
     */
    public void truncateDirtyFiles(long offset) {
        // 待移除的文件集合
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        // 遍历内部所有的MappedFile文件
        for (MappedFile file : this.mappedFiles) {
            // 获取当前文件自身的最大数据偏移量（每个MappedFile结束时偏移）=映射起始偏移量+文件大小
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 如果最大数据偏移量大于最大有效数据偏移量
            if (fileTailOffset > offset) {
                // 如果最大有效数据偏移量大于等于该文件的起始偏移量，则说明当前文件有一部分数据是有效的（包含有offset的文件，去掉后续尾巴），则设置该文件的有效属性
                if (offset >= file.getFileFromOffset()) {
                    // 设置当前文件的刷盘、提交、写入指针为当前最大有效数据偏移量（相对position位置）（之后的数据视为无效数据，可被覆盖）
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    /**
                     * 如果最大有效数据偏移量小于该文件的起始偏移量，则删除该文件
                     *  文件开头比offset大的，清除掉
                     *  关闭fileChannel，删除文件
                     */
                    file.destroy(1000);
                    // 记录到待删除的文件集合中
                    willRemoveFiles.add(file);
                }
            }
        }
        // 将等待移除的文件整体从mappedFiles中移除
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * MappedFileQueue#deleteExpiredFile(java.util.List)
     * 将删除的MappedFile从mappedFiles中淘汰
     * @param files 待移除的文件集合
     */
    void deleteExpiredFile(List<MappedFile> files) {
        // 遍历移除
        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    // 从mappedFiles集合中删除当前MappedFile
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                // 如果并没有完全移除这些无效文件，则记录异常信息
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }


    /**
     * MappedFileQueue#load()
     * 加载某个目录下的文件为MappedFile集合
     * 如果是 CommitLog：
     *   目录路径取自broker.conf文件中配置的storePathCommitLog属性
     *   默认值：{$ROCKETMQ_HOME}/store/commitlog/
     * @return
     */
    public boolean load() {
        /**
         * 存放目录
         * 情况一：获取消息日志文件（CommitLog日志文件）的存放目录
         *        目录路径取自broker.conf文件中配置的storePathCommitLog属性
         *        默认值：{$ROCKETMQ_HOME}/store/commitlog/
         * @return
         */
        File dir = new File(this.storePath);
        // 获取存储目录下的子文件集合
        File[] ls = dir.listFiles();
        if (ls != null) {
            // 如果存在子文件（commitlog文件），则加载子文件为MappedFile集合
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    /**
     * MappedFileQueue#doLoad(java.util.List)
     * 加载 File 为 MappedFile
     * @param files
     * @return
     */
    public boolean doLoad(List<File> files) {
        // ascending order
        /**
         * 根据文件名排序（文件名是20位的数字 - 偏移量 ）
         * 比如：
         *   对commitlog文件按照文件名生序排序
         */
        files.sort(Comparator.comparing(File::getName));
        // 遍历文件
        for (File file : files) {
            /**
             * 校验文件实际大小是否等于预定的文件大小
             *  如果不相等（文件大小非法），则直接返回false（加载失败），不再加载其它文件
             */
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                /*
                 * 核心代码
                 *   每一个commitlog文件都创建一个对应的MappedFile对象
                 *
                 */
                // 创建MappedFile
                MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                //
                /**
                 * 设置三个偏移量（wrotePosition 、flushedPosition、committedPosition）
                 * 默认值：文件大小
                 */
                // 当前文件所映射到的消息写入page cache的位置
                mappedFile.setWrotePosition(this.mappedFileSize);
                // 刷盘的最新位置
                mappedFile.setFlushedPosition(this.mappedFileSize);
                // 已提交的最新位置
                mappedFile.setCommittedPosition(this.mappedFileSize);
                // 将MappedFile添加到mappedFiles中
                //添加到MappedFileQueue内部的mappedFiles集合中
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * MappedFileQueue#getLastMappedFile(long, boolean)
     * 根据偏移量 创建或获取最后一个MappedFile（最新的）
     * @param startOffset  开始偏移量（起始offset）
     * @param needCreate   是否创建
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        // 从mappedFiles集合中获取最后一个MappedFile（最新的）
        MappedFile mappedFileLast = getLastMappedFile();
        /**
         * 如果为null（最后一个MappedFile（最新的）为空），则设置创建索引，默认为0（即新建的文件为第一个mappedFile文件，从0开始）
         * createOffset为MappedFile的开始偏移（新的最后一个MappedFile= 开始偏移量的整除结果）
         */
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        // 如果最后一个MappedFile（最新的）满了，则设置新mappedFile文件的创建索引 = 上一个文件的起始索引（即文件名） + mappedFileSize
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        // 如果需创建新mappedFile，则根据起始索引创建新的mappedFile
        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    /**
     * MappedFileQueue#tryCreateMappedFile(long)
     * 尝试创建某个偏移量的两个MappedFile（创建commitlog文件，映射MappedFile）
     * @param createOffset  起始索引（即新文件的文件名）
     * @return
     */
    protected MappedFile tryCreateMappedFile(long createOffset) {
        /**
         * 下一个文件路径：{storePathCommitLog}/createOffset（文件名=createOffset（即起始物理offset））
         * 文件名=偏移量
         * 文件名：20位，不足的话，则左边补0
         */
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        // 下下个文件的路径（文件名=偏移量+每个文件的大小）
        //
        /**
         * 下下一个文件路径 {storePathCommitLog}/createOffset+mappedFileSize（文件名（即起始offset）=createOffset + mappedFileSize）
         */
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        // 真正创建下一个文件和下下个文件
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    /**
     * 创建下一个文件和下下个文件
     * 创建commitlog文件，映射MappedFile
     * @param nextFilePath      要创建的下一个文件路径
     * @param nextNextFilePath    要创建的下下一个文件路径
     * @return
     */
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        /**
         * 如果allocateMappedFileService不为null，则异步的创建MappedFile
         * CommitLog的MappedFileQueue初始化时会初始化allocateMappedFileService，因此一般都不为null
         */
        if (this.allocateMappedFileService != null) {
            // 添加两个请求到处理任务池，然后阻塞等待异步创建默认1G大小的MappedFile
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                // 同步创建MappedFile
                mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            // 如果是第一次创建，则设置标志位firstCreateInQueue为true
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            // 将创建的mappedFile加入mappedFiles集合中
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    /**
     * MappedFileQueue#getLastMappedFile(long)
     *  根据偏移量获取最后一个MappedFile（最新的）
     *  创建新的MappedFile
     * @param startOffset  指定起始offset（开始偏移量）
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个MappedFile（最新的）
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                // 从mappedFiles中获取最后一个mappedFile
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * MappedFileQueue#getMaxOffset()
     * 获取最大偏移量
     * @return
     */
    public long getMaxOffset() {
        // 获取最后一个MappedFile
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 最大偏移量=最后一个MappedFile的映射开始位置+有效数据的最大位置
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * MappedFileQueue#deleteExpiredFileByTime(long, int, long, boolean)
     * 根据文件过期时间删除文件
     * @param expiredTime    文件过期时间（过期后保留的时间）
     * @param deleteFilesInterval  删除两个文件的间隔（默认100ms）
     * @param intervalForcibly  上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，默认120s）
     * @param cleanImmediately  是否强制删除文件
     * @return 删除文件数量
     */
    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        // 1、获取所有文件
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        // 需删除的文件集合
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            // 2、从0开始删除（越前面的时间，文件创建的越早）
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 2.1、计算文件应被删除的时间 = 文件最后修改的时间 + 文件过期时间
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                // 2.2、如果文件过期 或 开启强制删除，则删除文件
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    // 2.2.1、销毁文件
                    if (mappedFile.destroy(intervalForcibly)) {
                        // 2.2.1.1、删除文件
                        files.add(mappedFile);
                        // 2.2.1.1、删除文件数+1
                        deleteCount++;
                        // 一次最多删除10个文件
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }
                        // 每个文件删除间隔
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                // 删除睡眠
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        // 将删除的文件从mappedFiles中移除
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 根据偏移删除文件（删除小于最大偏移量的文件）
     * @param offset  要删除的最大偏移量
     * @param unitSize  ConsumeQueue中单个数据的大小（20字节）
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;
            // 循环遍历MappedFile
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取映射文件最后一个位置的索引（映射文件最后一条逻辑索引的字节内容）
                // 如果result == null，表明该映射文件还未填充完，即不存在下一个位置索引文件
                // 因此无需删除当前的位置索引文件
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    // 获取该位置索引所对应的  业务消息 开始物理位移
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    // 调用mappedFile.selectMappedBuffer方法时，持有计数器加1，
                    // 因此，查询完后，需释放引用，持有计数器减1。
                    result.release();
                    // 如果该位置索引文件的最大 业务消息物理位移 都比指定的offset小
                    // 则说明该位置索引文件可以删除
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    // 不可用，则删除
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                // 销毁文件（60s）
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        // 删除过期文件
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 执行刷盘
     * @param flushLeastPages  最少刷盘的页数
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        /**
         * 根据最新刷盘物理位置flushedWhere，去找到对应的MappedFile。
         * 如果flushedWhere为0：表示还未开始写消息，则获取第一个MappedFile
         */
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            // 获取存储时间戳，storeTimestamp在appendMessagesInner方法中被更新
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            /**
             * 执行刷盘操作
             */
            int offset = mappedFile.flush(flushLeastPages);
            // 获取最新刷盘物理偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            // 刷盘结果
            result = where == this.flushedWhere;
            // 更新刷盘物理位置
            this.flushedWhere = where;
            // 如果最少刷盘页数为0，则更新存储时间戳
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交刷盘
     * @param commitLeastPages    最少提交的页数
     * @return  n false表示提交了部分数据
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        /**
         * 根据最新提交物理位置committedWhere，去找到对应的MappedFile。
         *  如果committedWhere为0，表示还未开始提交消息，则获取第一个MappedFile
         */
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            /**
             * 执行提交操作
             */
            int offset = mappedFile.commit(commitLeastPages);
            // 获取最新提交物理偏移量
            long where = mappedFile.getFileFromOffset() + offset;
            // 如果不相等，表示提交了部分数据
            result = where == this.committedWhere;
            // 更新提交物理位置
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    /**
     * MappedFileQueue#findMappedFileByOffset(long, boolean)
     * 通过offset（偏移量）找到所在的mappedFile
     * @param offset                 目标偏移量
     * @param returnFirstOnNotFound  如果根据偏移量未找到目标文件，则返回第一个MappedFile（最老的）
     * @return   MappedFile 或者 null (当未找到且returnFirstOnNotFound为false时).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 获取第一个MappedFile（最老的）
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 获取最后一个MappedFile（最新的）
            MappedFile lastMappedFile = this.getLastMappedFile();
            // 第一个MappedFile、最后一个MappedFile都不为空的情况话，则进入
            if (firstMappedFile != null && lastMappedFile != null) {
                // 如果偏移量不再正确范围内（偏移量 < 第一个MappedFile的映射起始偏移量 或 偏移量 >= 最后一个MappedFile的映射最大偏移量），则打印异常日志
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                } else {
                    // 获取当前offset属于的MappedFile在mappedFiles集合中的索引位置
                    // 计算下标（该偏移量所在的文件）：（（偏移量/每个文件的大小）-（第一个文件的映射起始偏移量/每个文件的大小））
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        // 根据下标，获取对应的下标的MappedFile
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    // 如果指定offset在targetFile的offset范围内，则返回
                    // 目标文件不为空 & 目标偏移量>=目标文件的映射起始偏移量 & 目标偏移量<=目标文件结束偏移量（目标文件的映射起始偏移量+文件的大小）
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    // 遍历MappedFile集合，依次对每个MappedFile的offset范围进行判断，找到对应的tmpMappedFile并返回
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                // 到这里表示未找到任何MappedFile，如果returnFirstOnNotFound为true，则返回第一个文件
                if (returnFirstOnNotFound) {
                    // 遇到异常，允许返回第一个
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * MappedFileQueue#getFirstMappedFile()
     * 获取第一个MappedFile（最老的）
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                // 获取第一个MappedFile（最老的）
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    /**
     * MappedFileQueue#retryDeleteFirstFile(long)
     * 重新删除第一个文件
     * @param intervalForcibly 上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，默认120s）
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        // 获取第一个文件（MappedFile）
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            // 如果第一个文件（MappedFile）不可用，则删除该文件
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                // 销毁文件
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    // 销毁成功，则从mappedFiles中剔除
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
