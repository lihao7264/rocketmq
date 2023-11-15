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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    /**
     * 引用计数
     * 默认为1
     * >0：可用
     * <=0：不可用
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    /**
     * 是否可用
     * 默认为true
     */
    protected volatile boolean available = true;
    /**
     * 是否清理干净：关闭内存映射是否成功
     */
    protected volatile boolean cleanupOver = false;
    /**
     * 第一次删除的时间（第一次shutdown时间）
     * 默认为0
     */
    private volatile long firstShutdownTimestamp = 0;



    /**
     * ReferenceResource#hold()
     * 引用方法（占用资源）：使得引用次数 +1
     * 和release函数搭配使用
     * @return
     */
    public synchronized boolean hold() {
        // 是否可用
        if (this.isAvailable()) {
            // 如果可用，然后引用数+1，如果被加之前的引用数>0，则返回true
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                // 该分支基本不会走
                // 如果可用，如果被加之前的引用数<=0，则引用数-1
                this.refCount.getAndDecrement();
            }
        }
        // 返回false
        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * ReferenceResource#shutdown(long)
     * 关闭（清理）资源
     * 注意：如果在intervalForcibly时间内再次shutdown，代码不会执行任何逻辑
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            // 设置不可用
            this.available = false;
            // 第一次删除的时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 释放一个引用：MappedFile 上的引用-1、关闭内存映射
            this.release();
        } else if (this.getRefCount() > 0) {
            // 若 引用数>0
            // 如果 上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，CommitLog默认120s、IndexFile默认3s）
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 引用数设置为负数
                this.refCount.set(-1000 - this.getRefCount());
                //  MappedFile 上的引用-1、关闭内存映射
                this.release();
            }
        }
    }

    /**
     * ReferenceResource#release()
     * 和hold函数搭配
     * 释放引用：引用次数-1
     * 若计数 <=0，则调用cleanup，子类实现
     */
    public void release() {
        // 被引用数-1
        long value = this.refCount.decrementAndGet();
        // 被引用数大于0，则直接返回（还被使用）
        if (value > 0)
            return;
        // <=0，则清理文件
        synchronized (this) {
            // 关闭内存映射
            this.cleanupOver = this.cleanup(value);
        }
    }

    /**
     * 获取引用数
     * @return
     */
    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * ReferenceResource#isCleanupOver()
     * 是否清理干净：关闭内存映射是否成功
     * @return
     */
    public boolean isCleanupOver() {
        // 被引用数<=0 & 清理干净
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
