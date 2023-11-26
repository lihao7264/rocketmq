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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * index索引文件
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * hash槽字节数
     */
    private static int hashSlotSize = 4;
    /**
     * 索引字节数
     */
    private static int indexSize = 20;
    /**
     * 非法索引
     */
    private static int invalidIndex = 0;
    /**
     * hash槽数
     * 默认值：500w
     */
    private final int hashSlotNum;
    /**
     * 索引数
     * 默认值：500w * 4 = 2000w
     */
    private final int indexNum;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 索引文件头
     */
    private final IndexHeader indexHeader;

    /**
     * 创建IndexFile
     * @param fileName     文件名
     * @param hashSlotNum   哈希槽数，默认5000000
     * @param indexNum      索引数，默认5000000 * 4
     * @param endPhyOffset  上一个文件的endPhyOffset
     * @param endTimestamp  上一个文件的endTimestamp
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {
        /**
         * 文件大小，默认约400M左右
         * 40B 头数据 + 500w * 4B hashslot + 2000w * 20B index
         */
        int fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        // 构建mappedFile
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        // 生成DirectByteBuffer，对该buffer写操作会被反映到文件中
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        // 获取indexHeader
        this.indexHeader = new IndexHeader(byteBuffer);
        // 设置新文件的起始物理索引和结束物理索引都为上一个文件的结束物理索引
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }
        // 设置新文件的起始时间戳和结束时间戳都为上一个文件的结束时间戳
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }
    /**
     * IndexFile#destroy(long)
     * 销毁文件
     * @param intervalForcibly 上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，CommitLog默认120s、IndexFile默认30s）
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 构建Index索引
     * @param key             key
     * @param phyOffset       当前消息在commitlog中的物理偏移量
     * @param storeTimestamp  当前消息在commitlog中的消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 如果当前文件的index索引数量小于2000w，则表明当前文件还可以继续构建索引
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 计算Key的哈希值
            int keyHash = indexKeyHashMethod(key);
            // 通过 哈希值 & hash槽数 方式获取当前key对应的hash槽下标位置，hashSlotNum默认为2000w
            int slotPos = keyHash % this.hashSlotNum;
            // 计算该消息的绝对hash槽偏移量 absSlotPos = 40B + slotPos * 4B
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 获取当前hash槽的值，一个hash槽大小为4B
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 如果值不为0，则说明该hash key已存在（即存在hash冲突）
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                // 当前消息在commitlog中的消息存储时间与该Index文件起始时间差
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 单位：秒
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                // 获取该消息的索引存放位置的绝对偏移量：absIndexPos = 40B + 500w * 4B + indexCount * 20B
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;
                // 存入4B的当前消息的Key的哈希值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 存入8B的当前消息在commitlog中的物理偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 存入4B的当前消息在commitlog中的消息存储时间与该Index文件起始时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 存入4B的slotValue（即前面读出的 slotValue），可能是0，也可能不是0，而是上一个发生hash冲突的索引条目的编号
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 更新当前hash槽的值为最新的IndexFile的索引条目计数的编号（即当前索引存入的编号）
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                /*
                 * 从存入的数据可看出：
                 * IndexFile采用用slotValue字段将所有冲突的索引用链表的方式串起来，而哈希槽SlotTable并不保存真正的索引数据，
                 * 而是保存每个槽位对应的单向链表的头（即可看作是头插法插入数据）
                 */
                // 如果索引数小于等于1，说明该文件第一次存入索引，则初始化beginPhyOffset和beginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                // 如果slotValue为0，写入表示采用了一个新的哈希槽，此时hashSlotCount自增1
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                // 因为存入了新的索引，则索引条目计数indexCount自增1

                this.indexHeader.incIndexCount();
                //设置endPhyOffset和endTimestamp

                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
