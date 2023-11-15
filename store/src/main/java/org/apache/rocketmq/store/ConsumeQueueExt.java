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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Extend of consume queue, to store something not important,
 * such as message store time, filter bit map and etc.
 * <p/>
 * <li>1. This class is used only by {@link ConsumeQueue}</li>
 * <li>2. And is week reliable.</li>
 * <li>3. Be careful, address returned is always less than 0.</li>
 * <li>4. Pls keep this file small.</li>
 */

/**
 * ConsumeQueue类的拓展类，记录一些不重要的信息
 * 在MessageStoreConfig#enableConsumeQueueExt为true时生效(默认false)
 * 默认的存储路径：{storePathRootDir}/consumequeue_ext/{topic}/{queueId}/
 * 利用MappedFileQueue管理一个MappedFile队列,进行put、get、truncate、recover等操作
 * 每个mappedFile默认最大48M，存放CqExtUnit（存储单元：由头部、内容两部分组成）,标志位 short -1(代表数据结尾)，尾部预留4个字节作为END_BLANK
 * 整个consumequeue_ext记录的文件大小不得超过属性 MAX_REAL_OFFSET
 */
public class ConsumeQueueExt {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 对应的MappedFile队列（MappedFileQueue）
     */
    private final MappedFileQueue mappedFileQueue;
    /**
     * topic
     */
    private final String topic;

    /**
     * 队列id（queueId）
     */
    private final int queueId;
    /**
     * ConsumeQueueExt 的文件存储路径
     * {storePathRootDir}/consumequeue_ext
     */
    private final String storePath;
    /**
     * consumeQueueExt文件的最大大小（默认48MB）
     */
    private final int mappedFileSize;
    private ByteBuffer tempContainer;
    /**
     * 尾部预留4字节作为END_BLANK
     */
    public static final int END_BLANK_DATA_LENGTH = 4;

    /**
     * Addr can not exceed this value.For compatible.
     * -2147483649
     * Addr不能超过此值。
     * 对于兼容。
     * extAddr的上限
     */
    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;
    /**
     * 整个consumequeue_ext文件记录的偏移量上限（9223372034707292159）
     * 9223372034707292159
     */
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

    /**
     * Constructor.
     *
     * @param topic topic
     * @param queueId id of queue
     * @param storePath root dir of files to store.
     * @param mappedFileSize file size
     * @param bitMapLength bit map length.
     */
    public ConsumeQueueExt(final String topic,
                           final int queueId,
                           final String storePath,
                           final int mappedFileSize,
                           final int bitMapLength) {
        // 保存信息
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.topic = topic;
        this.queueId = queueId;
        // 默认的对应queueId的存储路径：{storePathRootDir}/consumequeue_ext/{topic}/{queueId}/
        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;
        // 创建MappedFileQueue
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        if (bitMapLength > 0) {
            // 默认创建8字节
            this.tempContainer = ByteBuffer.allocate(
                    bitMapLength / Byte.SIZE
            );
        }
    }

    /**
     * Check whether {@code address} point to extend file.
     * <p>
     * Just test {@code address} is less than 0.
     * </p>
     */
    /**
     * ConsumeQueueExt#isExtAddr(long)
     * 检查 address 是否指向扩展文件
     * @param address  地址
     * @return
     */
    public static boolean isExtAddr(final long address) {
        return address <= MAX_ADDR;
    }

    /**
     * Transform {@code address}(decorated by {@link #decorate}) to offset in mapped file.
     * <p>
     * if {@code address} is less than 0, return {@code address} - {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code address}
     * </p>
     */
    /**
     * ConsumeQueueExt#unDecorate(long)
     * 从mappedFile利用addr获取CqExtUnit时，需进行解码（一个正数）
     * @param address
     * @return
     */
    public long unDecorate(final long address) {
        if (isExtAddr(address)) {
            return address - Long.MIN_VALUE;
        }
        return address;
    }

    /**
     * Decorate {@code offset} from mapped file, in order to distinguish with tagsCode(saved in cq originally).
     * <p>
     * if {@code offset} is greater than or equal to 0, then return {@code offset} + {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code offset}
     * </p>
     *
     * @return ext address(value is less than 0)
     */
    /**
     * ConsumeQueueExt#decorate(long)
     * 编码方法
     * @param offset
     * @return 返回编码后的addr(一个负数)
     */
    public long decorate(final long offset) {
        if (!isExtAddr(offset)) {
            return offset + Long.MIN_VALUE;
        }
        return offset;
    }

    /**
     * Get data from buffer.
     *
     * @param address less than 0
     */
    /**
     * ConsumeQueueExt#get(long)
     * 根据修饰后的addr，获取mappedFile中对应位置描述的CqExtUnit
     * @param address
     * @return
     */
    public CqExtUnit get(final long address) {
        // 创建空CqExtUnit
        CqExtUnit cqExtUnit = new CqExtUnit();
        // 填充CqExtUnit
        if (get(address, cqExtUnit)) {
            return cqExtUnit;
        }

        return null;
    }

    /**
     * Get data from buffer, and set to {@code cqExtUnit}
     *
     * @param address less than 0
     */
    /**
     * ConsumeQueueExt#get(long, org.apache.rocketmq.store.ConsumeQueueExt.CqExtUnit)
     * 1、根据修饰后的addr，从MappedFileQueue中找到对应的file。
     * 2、通过 realOffset%mappedFileSize 获取到偏移。
     * 3、获取到buffer的切片副本 [pos，文件的截止位点]。
     * 4、读取buffer给cqExtUnit的数据结构赋值。
     * @param address
     * @param cqExtUnit
     * @return
     */
    public boolean get(final long address, final CqExtUnit cqExtUnit) {
        // 地址不指向扩展文件，则返回失败
        if (!isExtAddr(address)) {
            return false;
        }
        // mappedFile的最大大小（默认48MB）
        final int mappedFileSize = this.mappedFileSize;
        // 解码地址:真实偏移
        final long realOffset = unDecorate(address);
        // 根据realOffset获取到对应的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(realOffset, realOffset == 0);
        if (mappedFile == null) {
            return false;
        }
        // pos（文件中的偏移位置）=realOffset%mappedFileSize
        int pos = (int) (realOffset % mappedFileSize);
        // 获取文件中 [pos，文件的截止位点] 的mappedByteBuffer切片副本
        SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos);
        if (bufferResult == null) {
            log.warn("[BUG] Consume queue extend unit({}) is not found!", realOffset);
            return false;
        }
        boolean ret = false;
        try {
            // 将buffer数据读入到内存记录的数据结构中
            ret = cqExtUnit.read(bufferResult.getByteBuffer());
        } finally {
            // 释放buffer
            bufferResult.release();
        }

        return ret;
    }

    /**
     * Save to mapped buffer of file and return address.
     * <p>
     * Be careful, this method is not thread safe.
     * </p>
     *
     * @return success: < 0: fail: >=0
     */
    /**
     * ConsumeQueueExt#put(ConsumeQueueExt.CqExtUnit)
     * 存放一个cqExtUnit，返回编码后的地址
     * @param cqExtUnit
     * @return
     */
    public long put(final CqExtUnit cqExtUnit) {
        // 尝试三次
        final int retryTimes = 3;
        try {
            // 1、计算ConsumeQueueExt存储单元的大小
            int size = cqExtUnit.calcUnitSize();
            // 2、如果ConsumeQueueExt存储单元的大小 > CqExtUnit的头部+内容的最大大小(32767字节)，则返回失败
            // 太大了，超过了32k
            if (size > CqExtUnit.MAX_EXT_UNIT_SIZE) {
                log.error("Size of cq ext unit is greater than {}, {}", CqExtUnit.MAX_EXT_UNIT_SIZE, cqExtUnit);
                return 1;
            }
            // 3、文件最大偏移未达到 整个consumequeue_ext文件记录的偏移量上限（9223372034707292159）
            // ext文件记录数据过多
            if (this.mappedFileQueue.getMaxOffset() + size > MAX_REAL_OFFSET) {
                log.warn("Capacity of ext is maximum!{}, {}", this.mappedFileQueue.getMaxOffset(), size);
                return 1;
            }
            // unit size maybe change.but, the same most of the time.
            // 扩容ByteBuffer
            if (this.tempContainer == null || this.tempContainer.capacity() < size) {
                this.tempContainer = ByteBuffer.allocate(size);
            }

            for (int i = 0; i < retryTimes; i++) {
                // 获取最后一个MappedFile
                MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

                if (mappedFile == null || mappedFile.isFull()) {
                    // 之前最新的MappedFile写满了，则自动创建一个
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }

                if (mappedFile == null) {
                    log.error("Create mapped file when save consume queue extend, {}", cqExtUnit);
                    continue;
                }
                // 当前写文件位置
                final int wrotePosition = mappedFile.getWrotePosition();
                // 文件剩余的空余空间（文件大小-当前写文件位置-4字节（尾部预留4字节作为END_BLANK））
                final int blankSize = this.mappedFileSize - wrotePosition - END_BLANK_DATA_LENGTH;

                // check whether has enough space.
                // ConsumeQueueExt存储单元的大小>当前文件剩余的空余空间
                if (size > blankSize) {
                    // 标记该mappedFile为写满了
                    fullFillToEnd(mappedFile, wrotePosition);
                    log.info("No enough space(need:{}, has:{}) of file {}, so fill to end",
                            size, blankSize, mappedFile.getFileName());
                    // 重新再试
                    continue;
                }
                // mappedFile追加消息
                if (mappedFile.appendMessage(cqExtUnit.write(this.tempContainer), 0, size)) {
                    return decorate(wrotePosition + mappedFile.getFileFromOffset());
                }
            }
        } catch (Throwable e) {
            log.error("Save consume queue extend error, " + cqExtUnit, e);
        }

        return 1;
    }

    /**
     * ConsumeQueueExt#fullFillToEnd(MappedFile, int)
     * MappedFile写满了，结尾填充-1表示结束
     *   wrotePosition存放-1表示结束
     *   更新wrotePosition为mappedFileSize表示该mappedFile写满
     * @param mappedFile
     * @param wrotePosition
     */
    protected void fullFillToEnd(final MappedFile mappedFile, final int wrotePosition) {
        // 获取slice切片副本
        ByteBuffer mappedFileBuffer = mappedFile.sliceByteBuffer();
        mappedFileBuffer.position(wrotePosition);
        // wrotePosition存放-1表示结束
        // -1标记结束位
        // ending.
        mappedFileBuffer.putShort((short) -1);
        // 更新wrotePosition为mappedFileSize，表示该mappedFile写满（标记写满了）
        mappedFile.setWrotePosition(this.mappedFileSize);
    }

    /**
     * Load data from file when startup.
     */
    /**
     * ConsumeQueueExt#load()
     * 启动时，从文件加载ConsunmeQueueExt数据。
     * @return
     */
    public boolean load() {
        // 加载mappedFileQueue中的mappedFiles
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue extend" + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * ConsumeQueueExt#checkSelf()
     * 检查映射文件队列中的文件大小是否正确。
     * Check whether the step size in mapped file queue is correct.
     */
    public void checkSelf() {
        this.mappedFileQueue.checkSelf();
    }

    /**
     *ConsumeQueueExt#recover()
     * 恢复方法
     * Recover.
     */
    public void recover() {
        // 获取mappedFileQueue中的所有mappedFiles
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        // 如果为空，则返回
        if (mappedFiles == null || mappedFiles.isEmpty()) {
            return;
        }

        // load all files, consume queue will truncate extend files.
        // 加载所有文件，消费队列将截断扩展文件。
        int index = 0;
        // 第一个文件（MappedFile）
        MappedFile mappedFile = mappedFiles.get(index);
        // 获取分片副本
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        // 获取第一个文件（MappedFile）的映射起始偏移量
        long processOffset = mappedFile.getFileFromOffset();
        // 所有MappedFile中的数据的最大偏移量
        long mappedFileOffset = 0;
        // 创建空的CqExtUnit对象（ConsumeQueue扩展单元对象）
        CqExtUnit extUnit = new CqExtUnit();
        // 如下循环目的：获取最后一个mappedFile的映射起始偏移量
        while (true) {
            // 读取CqExtUnit中的size，跳过当前CqExtUnit
            // 在一个mappedFile之间skip
            extUnit.readBySkip(byteBuffer);

            // check whether write sth.
            // 如果size>0，则偏移量为mappedFileOffset+=size
            // 最后getSize()返回-1，文件尾部写入的short标志位
            if (extUnit.getSize() > 0) {
                mappedFileOffset += extUnit.getSize();
                continue;
            }
            // 当前文件遍历完毕，则遍历下一个文件
            index++;
            // 下标 小于 mappedFiles的大小（未到最后一个文件），则遍历下一个文件
            if (index < mappedFiles.size()) {
                // 下一个mappedFile
                mappedFile = mappedFiles.get(index);
                byteBuffer = mappedFile.sliceByteBuffer();
                // 下一个mappedFile的映射起始偏移量
                processOffset = mappedFile.getFileFromOffset();
                // 对应的偏移量为0
                mappedFileOffset = 0;
                log.info("Recover next consume queue extend file, " + mappedFile.getFileName());
                continue;
            }
            // 消费队列扩展的所有文件都已恢复到最后一个映射文件
            log.info("All files of consume queue extend has been recovered over, last mapped file "
                    + mappedFile.getFileName());
            break;
        }
        // 获取到最后一个mappedFile最后的绝对偏移（数据完结的偏移量）
        processOffset += mappedFileOffset;
        // 设置 已flush（刷盘）到的位置(某一个mappedFile中的一个位置（偏移地址）)
        this.mappedFileQueue.setFlushedWhere(processOffset);
        // 设置 已commit到的位置(某一个mappedFile中的一个位置（偏移地址）)
        this.mappedFileQueue.setCommittedWhere(processOffset);
        // 处理offset以上的脏 MappedFile
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    /**
     * Delete files before {@code minAddress}.
     *
     * @param minAddress less than 0
     */
    /**
     * ConsumeQueueExt#truncateByMinAddress(long)
     * 删除minAddress之前的mappedFile
     * @param minAddress 要保留的最小偏移地址（编码后的）
     */
    public void truncateByMinAddress(final long minAddress) {
        if (!isExtAddr(minAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by min {}.", minAddress);

        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        // 解码为真实的偏移地址
        final long realOffset = unDecorate(minAddress);
        // 遍历所有MappedFile，删除文件结尾偏移地址 小于 要保留的最小偏移地址的文件
        for (MappedFile file : mappedFiles) {
            // 文件结尾偏移地址 = 映射起始偏移量+文件大小
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 删除文件结尾偏移地址 小于 要保留的最小偏移地址的文件
            if (fileTailOffset < realOffset) {
                log.info("Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}", file.getFileName(),
                        fileTailOffset, realOffset);
                // 上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，CommitLog默认120s、IndexFile默认30s、ConsumeQueue默认60s、ConsumeQueueExt默认1s）
                if (file.destroy(1000)) {
                    willRemoveFiles.add(file);
                }
            }
        }
        // 从mappedFiles中删除对应的MappedFile
        this.mappedFileQueue.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * Delete files after {@code maxAddress}, and reset wrote/commit/flush position to last file.
     *
     * @param maxAddress less than 0
     */
    /**
     * ConsumeQueueExt#truncateByMaxAddress(long)
     * 删除maxAddress之后的mappedFile以及同一个mappedFile之后的记录
     * @param maxAddress 要保留的最大偏移地址（编码后的）
     */
    public void truncateByMaxAddress(final long maxAddress) {
        if (!isExtAddr(maxAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by max {}.", maxAddress);
        // 通过地址 获取mappedFile对应的CqExtUnit记录，用于获取size属性
        CqExtUnit cqExtUnit = get(maxAddress);
        if (cqExtUnit == null) {
            log.error("[BUG] address {} of consume queue extend not found!", maxAddress);
            return;
        }
        // 解码为真实的偏移地址
        final long realOffset = unDecorate(maxAddress);
        // 之后的偏移地址全部清空
        this.mappedFileQueue.truncateDirtyFiles(realOffset + cqExtUnit.getSize());
    }

    /**
     * flush buffer to file.
     */
    /**
     * ConsumeQueueExt#flush(int)
     * buffer刷盘
     * @param flushLeastPages 刷新最少的页数量
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    /**
     * delete files and directory.
     */
    /**
     * ConsumeQueueExt#destroy()
     * 销毁文件
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * Max address(value is less than 0).
     * <p/>
     * <p>
     * Be careful: it's an address just when invoking this method.
     * </p>
     */
    /**
     * ConsumeQueueExt#getMaxAddress()
     * 获取最大地址（编码后的）
     * @return
     */
    public long getMaxAddress() {
        // 获取最后一个MappedFile（最新的）
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            // 最后一个MappedFile为空的话，则返回偏移地址为0的编码后的地址
            return decorate(0);
        }
        // 返回 最后一个MappedFile的当前写文件位置 的编码后的地址
        return decorate(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
    }

    /**
     * Minus address saved in file.
     * ConsumeQueueExt#getMinAddress()
     * 获取文件中的最小地址（编码后的）
     */
    public long getMinAddress() {
        // 获取第一个MappedFile（最老的）
        MappedFile firstFile = this.mappedFileQueue.getFirstMappedFile();
        if (firstFile == null) {
            // 第一个文件为空的话，则返回偏移地址为0的编码后的地址
            return decorate(0);
        }
        // 返回 第一个 MappedFile的映射起始偏移量 的编码后的地址
        return decorate(firstFile.getFileFromOffset());
    }

    /**
     * Store unit.
     * ConsumeQueueExt的存储单元
     */
    public static class CqExtUnit {
        /**
         * 最小ConsumeQueueExt存储单元的大小为20字节
         * 这仅是CqExtUnit的头部大小
         */
        public static final short MIN_EXT_UNIT_SIZE
                = 2 * 1 // size, 32k max 消息大小 2个字节
                + 8 * 2 // msg time + tagCode  ，消息时间+tags的hashCode 16个字节
                + 2; // bitMapSize  bitMapSize 2个字节
        /**
         * 最大ConsumeQueueExt存储单元的大小为32767字节
         * CqExtUnit的头部+内容的最大大小
         */
        public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

        public CqExtUnit() {
        }


        public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
            this.tagsCode = tagsCode == null ? 0 : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            // size = 20（头信息） + bitMapSize（内容） ，规定不能超过32k（32767字节）
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
        }

        /**
         * unit size
         * ConsumeQueueExt存储单元的大小
         * size = 20（头部） + bitMapSize（内容） ，规定不能超过32k（32767字节）
         */
        private short size;
        /**
         * has code of tags
         * tags的hashCode
         */
        private long tagsCode;
        /**
         * the time to store into commit log of message
         * 消息存储的时间
         */
        private long msgStoreTime;
        /**
         * size of bit map
         * filterBitMap的大小
         */
        private short bitMapSize;
        /**
         * filter bit map
         * 内容
         */
        private byte[] filterBitMap;

        /**
         * ConsumeQueueExt.CqExtUnit#read(java.nio.ByteBuffer)
         * build unit from buffer from current position.
         * 将buffer数据读入到内存记录的数据结构中
         */
        private boolean read(final ByteBuffer buffer) {
            // 还未写size，则返回
            if (buffer.position() + 2 > buffer.limit()) {
                return false;
            }
            // 获取size
            this.size = buffer.getShort();
            // 数据为空，则返回
            if (this.size < 1) {
                return false;
            }
            // 获取tagsCode、msgStoreTime、bitMapSize
            this.tagsCode = buffer.getLong();
            this.msgStoreTime = buffer.getLong();
            this.bitMapSize = buffer.getShort();
            // bitMapSize（内容）< 1，则返回
            if (this.bitMapSize < 1) {
                return true;
            }
            // 获取filterBitMap
            if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
                this.filterBitMap = new byte[bitMapSize];
            }

            buffer.get(this.filterBitMap);
            return true;
        }

        /**
         * Only read first 2 byte to get unit size.
         * <p>
         * if size > 0, then skip buffer position with size.
         * </p>
         * <p>
         * if size <= 0, nothing to do.
         * </p>
         */
        /**
         * ConsumeQueueExt.CqExtUnit#readBySkip(java.nio.ByteBuffer)
         * 如果size>0，则buffer跳过对应长度的偏移量（跳到下一个CqExtUnit）
         * @param buffer
         */
        private void readBySkip(final ByteBuffer buffer) {
            // 使用slice()创建一个新的ByteBuffer
            ByteBuffer temp = buffer.slice();
            // temp读取 不会导致buffer.position变化
            short tempSize = temp.getShort();
            // 设置大小
            this.size = tempSize;
            // 修改下标
            if (tempSize > 0) {
                buffer.position(buffer.position() + this.size);
            }
        }

        /**
         * Transform unit data to byte array.
         * <p/>
         * <li>1. @{code container} can be null, it will be created if null.</li>
         * <li>2. if capacity of @{code container} is less than unit size, it will be created also.</li>
         * <li>3. Pls be sure that size of unit is not greater than {@link #MAX_EXT_UNIT_SIZE}</li>
         */
        /**
         * ConsumeQueueExt.CqExtUnit#write(java.nio.ByteBuffer)
         * 内存数据结构 放到 ByteBuffer中，转化成byte[]返回
         * @param container
         * @return
         */
        private byte[] write(final ByteBuffer container) {
            // bitMapSize大小
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            // size=20（头消息大小）+bitMapSize大小（内容大小）
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

            ByteBuffer temp = container;
            // 如果buffer的容量太小，则扩大buffer到当前CqExtUnit的大小
            if (temp == null || temp.capacity() < this.size) {
                temp = ByteBuffer.allocate(this.size);
            }
            // 切换为写模式
            temp.flip();
            // 将内存数据结构 写入到 ByteBuffer中
            temp.limit(this.size);

            temp.putShort(this.size);
            temp.putLong(this.tagsCode);
            temp.putLong(this.msgStoreTime);
            temp.putShort(this.bitMapSize);
            if (this.bitMapSize > 0) {
                temp.put(this.filterBitMap);
            }

            return temp.array();
        }

        /**
         * ConsumeQueueExt.CqExtUnit#calcUnitSize()
         * 计算ConsumeQueueExt存储单元的大小
         * Calculate unit size by current data.
         * @return
         */
        private int calcUnitSize() {
            // size=20（头消息）+filterBitMap（内容）大小
            int sizeTemp = MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
            return sizeTemp;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(final long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(final long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public byte[] getFilterBitMap() {
            if (this.bitMapSize < 1) {
                return null;
            }
            return filterBitMap;
        }

        /**
         * ConsumeQueueExt.CqExtUnit#setFilterBitMap(byte[])
         * 更新filterBitMap、bitMapSize
         * @param filterBitMap
         */
        public void setFilterBitMap(final byte[] filterBitMap) {
            this.filterBitMap = filterBitMap;
            // not safe transform, but size will be calculate by #calcUnitSize
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        }

        public short getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CqExtUnit))
                return false;

            CqExtUnit cqExtUnit = (CqExtUnit) o;

            if (bitMapSize != cqExtUnit.bitMapSize)
                return false;
            if (msgStoreTime != cqExtUnit.msgStoreTime)
                return false;
            if (size != cqExtUnit.size)
                return false;
            if (tagsCode != cqExtUnit.tagsCode)
                return false;
            if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) size;
            result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
            result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
            result = 31 * result + (int) bitMapSize;
            result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CqExtUnit{" +
                    "size=" + size +
                    ", tagsCode=" + tagsCode +
                    ", msgStoreTime=" + msgStoreTime +
                    ", bitMapSize=" + bitMapSize +
                    ", filterBitMap=" + Arrays.toString(filterBitMap) +
                    '}';
        }
    }
}
