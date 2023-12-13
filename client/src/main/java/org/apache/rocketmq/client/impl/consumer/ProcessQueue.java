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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 * 消费队列的处理队列
 */
public class ProcessQueue {
    /**
     * 客户端对于从broker获取的mq锁
     * 过期时间默认值：30s
     * 可通过-Drocketmq.client.rebalance.lockMaxLiveTime参数设置
     */
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    /**
     * 集群模式下，顺序消费会 尝试对所有分配给当前consumer的队列请求broker端的消息队列锁，保证同时只有一个消费端可消费
     * 默认值：每隔20s
     */
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    /**
     * 拉取消息超时
     * 默认值：120s
     */
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * treeMap操作的读写锁
     */
    private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
    /**
     * <消息偏移,消息>：（保存拉取到的消息）
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * 正在消费的消息数：放入时会加，移除时会减
     */
    private final AtomicLong msgCount = new AtomicLong();
    /**
     * 正在消费的消息总大小：
     */
    private final AtomicLong msgSize = new AtomicLong();
    /**
     * 顺序消费情况下，处理队列的消费锁（在顺序消费 和 顺序消费移除ProcessQueue时使用）
     */
    private final Lock consumeLock = new ReentrantLock();
    /**
     * 正在消费的顺序消息map
     * <消息偏移,消息信息>
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * 尝试解锁次数：记录废弃ProcessQueue时，lockConsume次数
     */
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    /**
     * 该队列下最大的消息偏移（ProcessQueue中保存的消息中最大offset，为ConsumeQueue的offset）
     */
    private volatile long queueOffsetMax = 0L;
    /**
     * 是否下线
     *  dropped = true：该队列中的消息将不会被消费
     */
    private volatile boolean dropped = false;
    /**
     * 最后的拉取时间戳（上次执行拉取消息的时间）
     */
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    /**
     * 最新消费消息的时间戳（上次消费完消息后记录的时间）
     */
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    /**
     * 是否锁成功
     */
    private volatile boolean locked = false;
    /**
     * 最新的加锁时间（上次锁定的时间）
     */
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    /**
     * 是否正在消费
     */
    private volatile boolean consuming = false;
    /**
     * 为调整线程池时，提供数据参考
     */
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * 拉取消息超时：最后一次拉取消息的时间跟现在超过120s
     * @return
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理过期消息
     * @param pushConsumer 消费者
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        // 如果是顺序消费，直接返回，只有并发消费才会清理
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }
        // 一次循环最多处理16个消息
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        // 遍历消息，最多处理前16个消息
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 加锁
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty()) {
                        /**
                         * 获取msgTreeMap中的第一次元素的起始消费时间
                         * msgTreeMap是一个红黑树，第一个节点是offset最小的节点
                         */
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        // 如果消费时间距离现在时间超过默认15min，则获取该msg
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            msg = msgTreeMap.firstEntry().getValue();
                        } else {
                            // 如果未被消费 或 消费时间距离现在时间不超过默认15min，则结束循环
                            break;
                        }
                    } else {
                        // msgTreeMap为空，结束循环
                        break;
                    }
                } finally {
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 将消息发回broker延迟topic，将在给定延迟时间（默认从level3（即10s开始））之后进行重试消费
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        // 如果该消息还未被消费完
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                // 移除消息
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 消息存入msgTreeMap这个红黑树map集合中
     * @param msgs  一批消息
     * @return  是否需分发消费，当前processQueue的内部的msgTreeMap中有消息 且 consuming=false（即还未开始消费）时，将会返回true
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            // 尝试加写锁防止并发
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    // 当该消息的偏移量 及 该消息存入msgTreeMap
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        // 如果集合没有该offset的消息，则增加统计数据
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                // 消息计数
                msgCount.addAndGet(validMsgCnt);
                // 当前processQueue内部的msgTreeMap中有消息 且 consuming=false（即还未开始消费）时，dispatchToConsume = true，consuming = true
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }
                // 计算broker累计消息数
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 移除执行集合中的所有消息，然后返回msgTreeMap中的最小的消息偏移量
     * @param msgs   需被移除的消息集合
     * @return   msgTreeMap中的最小的消息偏移量
     */
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 获取写锁
            this.treeMapLock.writeLock().lockInterruptibly();
            // 更新时间戳
            this.lastConsumeTimestamp = now;
            try {
                // 如果msgTreeMap存在数据
                if (!msgTreeMap.isEmpty()) {
                    // 将result设置为该队列最大的消息偏移量+1
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    // 遍历每一条消息尝试移除
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);
                    // 如果移除消息后，msgTreeMap不为空集合，则result设置为msgTreeMap当前最小的消息偏移量
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public void rollback() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     *  顺序消费调用的方法，删除processQueue中消费完毕的消息，返回待更新的offset
     * @return  待更新的offset
     */
    public long commit() {
        try {
            // 加锁
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                // 获取正在消费的消息map中的最大消息offset
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                // 正在消费的消息数
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());
                // msgSize 减去 已消费完成的消息大小
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                // 清空正在消费的消息map
                this.consumingMsgOrderlyTreeMap.clear();
                // 返回下一个offset，已消费完毕的最大offset + 1
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 顺序消费调用，标记消息等待再次消费
     * @param msgs  标记的消息
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            // 加锁
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                // 遍历消息
                for (MessageExt msg : msgs) {
                    // 从正在消费的consumingMsgOrderlyTreeMap中移除该消息
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    // 重新存入待消费的msgTreeMap中，则将会在随后的消费中被拉取，进而实现重复消费
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                // 解锁
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 拉取消息
     * 顺序消息使用的方法
     * 从processQueue中的msgTreeMap有序map集合中获取offset最小的consumeBatchSize条消息
     * 按顺序从最小的offset返回，保证有序性
     * @param batchSize  批量消费数
     * @return  拉取的消息
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            // 加锁
            this.treeMapLock.writeLock().lockInterruptibly();
            /**
             * 最新消费消息的时间
             */
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // 循环batchSize次
                    for (int i = 0; i < batchSize; i++) {
                        // 每次都拉取msgTreeMap中最小的一条消息
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            // 将拉取到的消息存入consumingMsgOrderlyTreeMap中，表示正在消费的消息
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }
                // 如果未拉取到任何一条消息，则设置consuming为false（表示没有消息，处于非消费状态）
                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                // 解锁
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.treeMapLock.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.treeMapLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getConsumeLock() {
        return consumeLock;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.treeMapLock.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.treeMapLock.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
