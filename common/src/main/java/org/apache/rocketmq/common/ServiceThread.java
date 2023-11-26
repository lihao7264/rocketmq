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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 服务线程基础类
 * 子类如下：
 * AllocateMappedFileService
 * StoreStatsService
 * GroupCommitService
 * FlushRealTimeService
 * CommitRealTimeService
 * FlushConsumeQueueService
 * FlushCommitLogService
 * FlushDiskWatcher
 * ReputMessageService
 * 等等
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;
    /**
     * 内部线程
     */

    private Thread thread;
    /**
     * 等待点（刷盘后，利用该等待一段时间）
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 已通知标志位
     * true：服务线程曾被尝试唤醒过 或 wakeup()方法曾被调用过（即此前曾有过消息存储的请求）
     * false：服务线程曾未被尝试唤醒过 或 wakeup()方法曾未被调用过（即此前这段时间未提交过消息存储的请求）
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 是否停止
     */
    protected volatile boolean stopped = false;
    /**
     * 是否是守护线程
     */
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    /**
     * 是否启动
     * 保证只启动一次
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     *  启动一个线程执行线程任务
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 只能启动一次
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        // 新建线程
        this.thread = new Thread(this, getServiceName());
        // 后台线程
        this.thread.setDaemon(isDaemon);
        // 启动线程
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 尝试唤醒等待的线程
     */
    public void wakeup() {
        // 尝试CAS的将已通知标志位从false改为true
        if (hasNotified.compareAndSet(false, true)) {
            // 如果成功则通知刷盘服务线程，如果失败则表示此前已通知过
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待执行刷盘，同步和异步刷盘服务都会调用该方法
     * @param interval  时间
     */
    protected void waitForRunning(long interval) {
        // 尝试CAS的将已通知标志位从true改为false，表示正在或已执行刷盘操作
        if (hasNotified.compareAndSet(true, false)) {
            /**
             * 如果成功则表示服务线程曾被尝试唤醒过 或 wakeup()方法曾被调用过（即此前曾有过消息存储的请求）
             *  则此时直接调用onWaitEnd方法交换读写队列，为后续消息持久化做准备
             */
            this.onWaitEnd();
            return;
        }
        /**
         * 进入这里表示CAS失败（即已通知标志位已是false）
         * 表示服务线程曾未被尝试唤醒过 或 wakeup()方法曾未被调用过（即此前这段时间未提交过消息存储的请求）
         */
        //entry to wait
        // 重置倒计数
        waitPoint.reset();

        try {
            /**
             * 由于此前没有刷盘请求被提交过，则刷盘服务线程等待一定时间，减少资源消耗
             * 同步刷盘服务最多等待10ms
             */
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 等待时间到了 或 因为刷盘请求而被唤醒，此时将已通知标志位直接改为false，表示正在或已执行刷盘操作
            hasNotified.set(false);
            // 调用onWaitEnd方法交换读写队列，为后续消息持久化做准备，一定会尝试执行一次刷盘操作
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
