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

/**
 * 描述选取MappedFile中 一部分ByteBuffer 的结果，提供随机读的操作
 */
public class SelectMappedBufferResult {

    /**
     * 开始位置：绝对偏移，20位long型
     */
    private final long startOffset;

    /**
     * 对应的byteBuffer
     */
    private final ByteBuffer byteBuffer;

    /**
     * 对应的byteBuffer的大小
     */
    private int size;

    /**
     * 属于哪个mappedFile的一部分
     */
    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        // 修改限制
        this.byteBuffer.limit(this.size);
    }

    /**
     * SelectMappedBufferResult#release()
     * 释放资源
     */
    public synchronized void release() {
        if (this.mappedFile != null) {
            // 释放mappedFile引用
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
