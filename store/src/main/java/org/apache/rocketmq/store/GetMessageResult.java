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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 获取消息的结果
 */
public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList;
    /**
     * 消息集合
     */
    private final List<ByteBuffer> messageBufferList;

    /**
     * 拉取消息的状态
     */
    private GetMessageStatus status;
    /**
     * 下次拉取的consumeQueue的起始逻辑偏移量
     */
    private long nextBeginOffset;
    /**
     * 最小的逻辑偏移量offset
     */
    private long minOffset;
    /**
     * 最大的逻辑偏移量offset
     */
    private long maxOffset;

    /**
     * 总消息大小
     */
    private int bufferTotalSize = 0;

    /**
     * 是否建议从SLAVE拉取消息
     * 默认值：不建议
     */
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMapedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    /**
     * 添加消息
     * @param mapedBuffer  消息buffer
     */
    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        // mapedBuffer加入到集合中
        this.messageMapedList.add(mapedBuffer);
        // byteBuffer加入到集合中
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        // 已拉取的消息总大小加上当前消息的大小
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
