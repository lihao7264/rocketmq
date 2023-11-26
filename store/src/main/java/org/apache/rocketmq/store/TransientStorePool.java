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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 临时存储池
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 临时存储池中buffer数
     * 默认值：5个
     */
    private final int poolSize;

    /**
     * CommitLog 文件的默认大小是1G
     */
    private final int fileSize;
    /**
     * 可用buffer双端队列
     */
    private final Deque<ByteBuffer> availableBuffers;
    /**
     * 消息存储配置
     */
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        // 默认5个
        for (int i = 0; i < poolSize; i++) {
            // 分配堆外内存，默认大小1G
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 锁定堆外内存，确保不会被置换到虚拟内存中去
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
            // 存入队列中
            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 从临时缓存池中借buffer
     * @return
     */
    public ByteBuffer borrowBuffer() {
        // 获取一个临时缓存池中的buffer
        ByteBuffer buffer = availableBuffers.pollFirst();
        // 如果可用的buffer数少于40%，则告警
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 可用buffers数
     * @return
     */
    public int availableBufferNums() {
        // 如果启动，则返回可用的堆外内存池数
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        // 如果没开启则返回最大int值
        return Integer.MAX_VALUE;
    }
}
