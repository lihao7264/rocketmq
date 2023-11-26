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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
// 固定40个字节
public class IndexHeader {
    /**
     * IndexHeader的大小（固定40个字节）
     */
    public static final int INDEX_HEADER_SIZE = 40;
    /**
     * 第一条消息的存储时间的偏移位置
     * 4*8字节+2*4字节=40字节
     */
    private static int beginTimestampIndex = 0;
    /**
     *
     */
    private static int endTimestampIndex = 8;
    private static int beginPhyoffsetIndex = 16;
    private static int endPhyoffsetIndex = 24;
    /**
     * hash槽数
     */
    private static int hashSlotcountIndex = 32;
    /**
     * 索引文件头中索引数的下标
     */
    private static int indexCountIndex = 36;
    // 对应的ByteBuffer
    private final ByteBuffer byteBuffer;
    // 该indexFile中第一条消息的存储时间
    private final AtomicLong beginTimestamp = new AtomicLong(0);
    // 该indexFile中最后一条消息存储时间
    private final AtomicLong endTimestamp = new AtomicLong(0);
    // 该indexFile中第一条消息在commitlog中的偏移量commitlog offset
    private final AtomicLong beginPhyOffset = new AtomicLong(0);
    // 该indexFile中最后一条消息在commitlog中的偏移量commitlog offset
    private final AtomicLong endPhyOffset = new AtomicLong(0);
    // 已填充有index的slot数量（并非每个slot槽下都挂载有index索引单元，这里统计的是所有挂载了index索引单元的slot槽的数量）
    private final AtomicInteger hashSlotCount = new AtomicInteger(0);
    // 该indexFile中包含的索引单元个数（统计出当前indexFile中所有slot槽下挂载的所有index索引单元的数量之和）
    private final AtomicInteger indexCount = new AtomicInteger(1);

    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * 根据byteBuffer加载相关的IndexFile的头信息（读取byteBuffer的内容到内存中）
     */
    public void load() {
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    /**
     * 更新header的各个记录
     */
    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }
    /**
     * 已填充有index的slot数量 + 1
     */
    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    /**
     * 对应的索引文件记录的索引单元个数 + 1
     */
    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
