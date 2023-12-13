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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 消费队列
 */
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息进度队列的存储单元大小为20字节
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;
    /**
     * MappedFile队列
     */
    private final MappedFileQueue mappedFileQueue;
    /**
     * 消费队列对应的topic
     */
    private final String topic;
    /**
     * 消费队列id
     */
    private final int queueId;
    /**
     * ConsumeQueue对应的byteBuffer的下标
     * 分配20个字节的堆外内存，用于临时存储单个索引
     * 注意：这段内存可循环使用
     */
    private final ByteBuffer byteBufferIndex;

    /**
     * ConsumeQueue 的文件存储地址
     * {storePathRootDir}/consumequeue
     */
    private final String storePath;
    /**
     * mappedFile的最大大小
     */
    private final int mappedFileSize;
    /**
     * 当前queueId目录下，所有ConsumeQueue文件中的最大有效物理偏移量
     */
    private long maxPhysicOffset = -1;
    /**
     * 消费者端不能消费小于minLogicOffset的业务消息
     */
    private volatile long minLogicOffset = 0;
    /**
     * ConsumeQueueExt:ConsumeQueue的拓展信息（记录一些不重要的信息）
     * MessageStoreConfig#enableConsumeQueueExt为true，才启用
     */
    private ConsumeQueueExt consumeQueueExt = null;

    /**
     * 创建ConsumeQueue
     * @param topic
     * @param queueId
     * @param storePath
     * @param mappedFileSize
     * @param defaultMessageStore
     */
    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final DefaultMessageStore defaultMessageStore) {
        //各种属性
        this.storePath = storePath;
        // 单个文件大小，默认为可存储30W数据的大小，每条数据20Byte
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;
        // topic
        this.topic = topic;
        // 队列id
        this.queueId = queueId;
        // queue的路径：{$ROCKETMQ_HOME}/store/consumequeue/{topic}/{queueId}/{fileName}
        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;
        // 创建mappedFileQueue，内部保存在该queueId下的所有的consumeQueue文件集合mappedFiles相当于一个文件夹
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        // 分配20个字节的堆外内存，用于临时存储单个索引，这段内存可循环使用
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        /**
         * 是否启用消息队列的扩展存储
         * 默认值：false
         */
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 加载ConsumeQueue文件
     *  对自己管理的队列id目录下的ConsumeQueue文件进行加载
     * @return
     */
    public boolean load() {
        // 调用mappedFileQueue的load方法，会对每个ConsumeQueue文件床创建一个MappedFile对象 并 进行内存映射mmap操作。
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            // 扩展加载，扩展消费队列用于存储不重要的东西（比如：消息存储时间、过滤位图等）。
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 恢复ConsumeQueue
     */
    public void recover() {
        // 获取所有的ConsumeQueue文件映射的mappedFiles集合
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            // 不足3个文件，则直接从第一个文件开始恢复。
            if (index < 0)
                index = 0;
            // ConsumeQueue映射文件的文件大小
            int mappedFileSizeLogics = this.mappedFileSize;
            // 获取文件对应的映射对象
            MappedFile mappedFile = mappedFiles.get(index);
            // 文件映射对应的DirectByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 获取文件映射的初始物理偏移量（和文件名相同）
            long processOffset = mappedFile.getFileFromOffset();
            // consumequeue映射文件的有效offset
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                /**
                 * 校验每一个索引条目的有效性
                 * CQ_STORE_UNIT_SIZE：每个条目（消息进度队列的存储单元）的大小，默认值20字节
                 */
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    // 获取该条目对应的消息在commitlog文件中的物理偏移量
                    long offset = byteBuffer.getLong();
                    // 获取该条目对应的消息在commitlog文件中的总长度
                    int size = byteBuffer.getInt();
                    // 获取该条目对应的消息的tag哈希值
                    long tagsCode = byteBuffer.getLong();
                    // 如果offset和size都大于0。则表示当前条目有效
                    if (offset >= 0 && size > 0) {
                        // 更新当前ConsumeQueue文件中的有效数据偏移量
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        // 更新当前queueId目录下的所有ConsumeQueue文件中的最大有效物理偏移量
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        // 否则，表示当前条目无效了，后续条目不会遍历
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                // 如果当前ConsumeQueue文件中的有效数据偏移量和文件大小一样，则表示该ConsumeQueue文件的所有条目都是有效的
                if (mappedFileOffset == mappedFileSizeLogics) {
                    // 校验下一个文件
                    index++;
                    // 遍历到了最后一个文件，则结束遍历
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        // 获取下一个文件的数据
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    // 如果不相等，则表示当前ConsumeQueue有部分无效数据，恢复结束
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }
            // 该文件映射的已恢复的物理偏移量
            processOffset += mappedFileOffset;
            /**
             *设置当前queueId下的所有的ConsumeQueue文件的最新数据
             *  设置刷盘最新位置，提交的最新位置
             */
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            /*
             * 删除文件最大有效数据偏移量（processOffset）之后的所有数据
             */
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    public void truncateDirtyLogicFiles(long phyOffset) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffset;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffset) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * ConsumeQueue#deleteExpiredFile(long)
     * 删除过期文件：找到所有mappedFile中最后一条记录的offset（maxOffset） < 参数offset的mappedFile，然后删掉
     * @param offset  要保留的最小偏移量
     * @return
     */
    public int deleteExpiredFile(long offset) {
        // 1、找到所有mappedFile中最后一条记录的offset（maxOffset） < 参数offset（当前剩余CommitLog的最小偏移地址）的mappedFile，然后删掉
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        // 2、校正小于minOffset的【逻辑位移索引】
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * ConsumeQueue#correctMinOffset(long)
     * 校正小于minOffset的【逻辑位移索引】
     * 该文件中的minOffset<phyMinOffset<maxOffset，该方法会让 minOffset到phyMinOffset之间的数据不能使用
     * @param phyMinOffset
     */
    public void correctMinOffset(long phyMinOffset) {
        // step1：获取第一个存储文件，一定有一部分的【逻辑位移索引】所关联的【业务消息的存储物理位移】一定大于业务消息最小的存储物理位移minOffset
        // 可能导致部分小于minOffset的【逻辑位移索引】存在于文件中
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result == null) {
                return;
            }
            try {
                // step2：从头开始遍历该存储文件中所有的【逻辑位移索引】，直到找到大于或等于minOffset的【逻辑位移索引】
                for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    long offsetPy = result.getByteBuffer().getLong();
                    result.getByteBuffer().getInt();
                    long tagsCode = result.getByteBuffer().getLong();
                    // step3：以该【逻辑位移索引】的物理位移更新至ConsumerQueue实例中的minLogicOffset属性即可
                    if (offsetPy >= phyMinOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has extend file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                // 释放SelectMappedBufferResult
                result.release();
            }
        }
        // 如果启用了consumeQueueExt，则删除minAddress之前的mappedFile
        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 将消息信息追加到ConsumeQueue索引文件中
     * @param request
     * @param multiQueue
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request, boolean multiQueue) {
        // 最大重试次数30
        final int maxRetries = 30;
        // 检查ConsumeQueue文件是否可写
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        // 如果文件可写 且 重试次数小于30次则写入ConsumeQueue索引
        for (int i = 0; i < maxRetries && canWrite; i++) {
            // 获取tagCode
            long tagsCode = request.getTagsCode();
            /**
             * 如果支持扩展信息写入
             * 默认值：false
             */
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                            topic, queueId, request.getCommitLogOffset());
                }
            }
            /**
             * 写入消息位置信息到ConsumeQueue中
             */
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                    request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                        this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    // 修改StoreCheckpoint中的physicMsgTimestamp：最新commitlog文件的刷盘时间戳，单位毫秒
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                if (multiQueue) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.defaultMessageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);

        }
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
                                    int queueId) {
        ConsumeQueue cq = this.defaultMessageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = cq.putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                    request.getTagsCode(),
                    queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    /**
     *  写入消息位置信息到ConsumeQueue中
     * @param offset      消息在CommitLog中的物理偏移量
     * @param size        消息大小
     * @param tagsCode    消息tagsCode（延迟消息：消息投递时间、其他消息：消息的tags的hashCode）
     * @param cqOffset    消息在消息消费队列的偏移量
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {

        /**
         * 如果消息偏移量+消息大小 小于等于 ConsumeQueue已处理的最大物理偏移量
         * 说明该消息已被写过，直接返回true
         */
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }
        /**
         * 将消息信息offset、size、tagsCode按照顺序存入临时缓冲区byteBufferIndex中
         */
        //position指针移到缓冲区头部
        this.byteBufferIndex.flip();
        // 缓冲区的限制20B
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        // 存入8字节长度的offset，消息在CommitLog中的物理偏移量
        this.byteBufferIndex.putLong(offset);
        // 存入4字节长度的size，消息大小
        this.byteBufferIndex.putInt(size);
        // 存入8字节长度的tagsCode延迟消息：消息投递时间、其他消息：消息tags的hashCode）
        this.byteBufferIndex.putLong(tagsCode);
        // 已存在索引数据的最大预计偏移量
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        /**
         * 根据偏移量获取将要写入的最新ConsumeQueue文件的MappedFile，可能会新建ConsumeQueue文件
         */
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            /**
             * 如果mappedFile是第一个创建的消费队列 且 消息在消费队列的偏移量不为0 且 消费队列写入指针为0
             * 则表示消费索引数据错误，需重设索引信息
             */
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                // 设置最小偏移量为预计偏移量
                this.minLogicOffset = expectLogicOffset;
                // 设置刷盘最新位置，提交的最新位置
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                // 对该ConsumeQueue文件expectLogicOffset之前的位置填充前导0
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }
            // 如果消息在消费队列的偏移量不为0（即此前有数据）
            if (cqOffset != 0) {
                // 获取当前ConsumeQueue文件最新已写入物理偏移量
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
                // 最新已写入物理偏移量大于预期偏移量，则表示重复构建消费队列
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }
                // 如果不相等，表示存在写入错误，正常情况下，两个值应相等，因为一个索引条目固定大小20B
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }
            // 更新消息最大物理偏移量 = 消息在CommitLog中的物理偏移量 + 消息的大小
            this.maxPhysicOffset = offset + size;
            /**
             * 将临时缓冲区中的索引信息追加到mappedFile的mappedByteBuffer中 并 更新wrotePosition的位置信息，到此构建ComsumeQueue完毕
             */
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 获取索引数据buffer
     * 根据消费点位（ConsumeQueue中的第几个存储单元）定位到物理偏移量，然后截取缓冲区（包含要拉取的消息的索引数据 及其 MappedFile之后的全部数据）
     * @param startIndex  起始消费点位（ConsumeQueue中的第几个存储单元）
     * @return   截取的索引缓存区
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        // 物理偏移量
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        // 如果大于ConsumeQueue的最小物理偏移量
        if (offset >= this.getMinLogicOffset()) {
            // 根据物理偏移量查找ConsumeQueue文件对应的MappedFile
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                // 从该MappedFile中截取一段ByteBuffer，这段内存存储着将要拉取的消息的索引数据及其之后的全部数据
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    /**
     * 如果支持扩展信息写入
     * 默认值：false
     */
    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

}
