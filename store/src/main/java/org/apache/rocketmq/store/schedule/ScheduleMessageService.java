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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 处理RocketMQ延迟消息的服务
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 首次延迟时间
     */
    private static final long FIRST_DELAY_TIME = 1000L;
    /**
     * 第二次延迟时间（即100ms后再次执行消息投递失败）
     */
    private static final long DELAY_FOR_A_WHILE = 100L;
    /**
     * 执行消息投递失败，则延迟10s再次执行消息投递
     */
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private static final long DELAY_FOR_A_SLEEP = 10L;
    /**
     * <level,延迟时间（1m：1 * 1000L * 60）>
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);
    /**
     * <level,偏移量(offset)>
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    /**
     * 是否启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);
    /**
     * 延迟消息投递线程池
     * 核心线程数：最大的延迟等级
     * 默认值：18
     */
    private ScheduledExecutorService deliverExecutorService;
    /**
     * writeMessageStore 与 defaultMessageStore指向同一个引用
     */
    private MessageStore writeMessageStore;
    /**
     * 最大延迟级别
     */
    private int maxDelayLevel;
    /**
     * 异步投递
     * 默认值：false
     */
    private boolean enableAsyncDeliver = false;

    /**
     * 延迟消息异步投递线程池
     * 核心线程数：最大的延迟等级
     * 默认值：18
     */
    private ScheduledExecutorService handleExecutorService;
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
            new ConcurrentHashMap<>(32);

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
        if (defaultMessageStore != null) {
            this.enableAsyncDeliver = defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        }
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    /**
     * 启动调度消息服务
     */
    public void start() {
        // 将启动标志CAS的从false改为true，该服务只能启动一次
        if (started.compareAndSet(false, true)) {
            /**
             * 调用父类的load方法，将延迟消息文件${ROCKETMQ_HOME}/store/config/delayOffset.json加载到内存的offsetTable集合中
             *  fix(dledger): reload the delay offset when master changed (#2518)
             */
            this.load();
            /**
             * 1.初始化延迟消息投递线程池
             * 核心线程数：最大的延迟等级
             * 默认值：18
             */
            this.deliverExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
            // 异步投递，默认不支持
            if (this.enableAsyncDeliver) {
                /**
                 * 1.初始化延迟消息异步投递线程池
                 * 核心线程数：最大的延迟等级
                 * 默认值：18
                 */
                this.handleExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
            }
            /**
             * 2.对所有的延迟等级构建一个对应的DeliverDelayedMessageTimerTask调度任务
             * 默认延迟1000ms后执行
             */
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                // 延迟等级
                Integer level = entry.getKey();
                // 延迟时间，毫秒
                Long timeDelay = entry.getValue();
                // 根据延迟等级获取对应的延迟队列的消费偏移量，如果没有则设置为0
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }
                // 延迟时间不为null，则为该等级的延迟队列构建一个DeliverDelayedMessageTimerTask调度任务，默认延迟1000ms后执行
                if (timeDelay != null) {
                    if (this.enableAsyncDeliver) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                    // DeliverDelayedMessageTimerTask构造参数包括对应的延迟等级 及 最新消费偏移量
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
            }
            /**
             * 3.构建一个延迟队列消费偏移量持久化的定时调度任务
             *   首次延迟1000ms之后执行
             *   后续每次执行间隔flushDelayOffsetInterval时间（默认10s）
             */
            this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
            try {
                this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("deliverExecutorService awaitTermination error", e);
            }

            if (this.handleExecutorService != null) {
                this.handleExecutorService.shutdown();
                try {
                    this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("handleExecutorService awaitTermination error", e);
                }
            }

            if (this.deliverPendingTable != null) {
                for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
                    log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
                }
            }

            this.persist();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 加载延迟消息数据，初始化delayLevelTable（延时等级）和offsetTable、配置文件等。
     * @return
     */
    @Override
    public boolean load() {
        /**
         * 调用父类ConfigManager#load方法，将延迟消息文件${ROCKETMQ_HOME}/store/config/delayOffset.json加载到内存的offsetTable集合中
         * delayOffset.json中保存着延迟topic每个队列的消费进度（消费偏移量）
         */
        boolean result = super.load();
        // 加载延时等级：解析延迟级别到delayLevelTable集合中
        result = result && this.parseDelayLevel();
        // 加载延时偏移量：校正每个延迟队列的偏移量
        result = result && this.correctDelayOffset();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            for (int delayLevel : delayLevelTable.keySet()) {
                ConsumeQueue cq =
                        ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                                delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    /**
     * 获取延迟消息文件路径${ROCKETMQ_HOME}/store/config/delayOffset.json
     * @return
     */
    @Override
    public String configFilePath() {
        /**
         * ${ROCKETMQ_HOME}/store/config/delayOffset.json
         * 举例；
         * {
         *   "offsetTable":{3:2,4:1
         *   }
         * }
         */
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    /**
     * json字符串转换为offsetTable对象
     * @param jsonString
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 解析延迟等级到delayLevelTable中
     * @return
     */
    public boolean parseDelayLevel() {
        // 时间单位表
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
        // 从MessageStoreConfig中获取延迟等级字符串（messageDelayLevel）
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            // 通过空格拆分
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                // 获取每个等级的延迟时间
                String value = levelArray[i];
                // 获取延迟单位
                String ch = value.substring(value.length() - 1);
                // 获取对应的延迟单位的时间毫秒
                Long tu = timeUnitTable.get(ch);
                // 延迟等级，从1开始
                int level = i + 1;
                // 如果当前等级已大于最大等级，则赋值为最大延迟级别
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                // 延迟时间
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                // 计算该等级的延迟时间毫秒
                long delayTimeMillis = tu * num;
                // 存入delayLevelTable中
                this.delayLevelTable.put(level, delayTimeMillis);
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * 恢复正常消息
     * 还原原始消息，设置topic为REAL_TOPIC属性值（即原始topic），设置queueId为REAL_QID属性值（即原始queueId）。
     * 即恢复为正常消息
     * @param msgExt  延迟消息
     * @return  真实消息
     */
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        // 构建MessageExtBrokerInner对象
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        // 延迟消息的tagsCode为投递时间，现在计算真正的tagsCodeValue
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        // 无需等待存储完成后才返回
        msgInner.setWaitStoreMsgOK(false);
        // 清除延时等级
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        // 设置回原来的 topic、queueId
        // 设置topic的值为REAL_TOPIC属性值（即原始topic），可能是重试topic或真实topic
        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
        // 设置queueId的值为REAL_QID属性值（即原始queueId），可能是重试queueId 或 真实queueId
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    /**
     * 投递延迟消息任务
     */
    class DeliverDelayedMessageTimerTask implements Runnable {
        /**
         * 延迟级别
         */
        private final int delayLevel;
        /**
         * 消费点位
         */
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                // 如果服务已启动，则继续执行
                if (isStarted()) {
                    // 执行消息投递
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                /**
                 * 抛出异常，新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService
                 * 10000ms后执行，本次任务结束
                 */
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 校验投递时间
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            // 投递时间戳
            long result = deliverTimestamp;
            // 当前时间 + 延迟时间
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            // 保证投递时间小于等于当前时间 + 延迟时间
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        /**
         * 执行延迟消息投递
         */
        public void executeOnTimeup() {
            /**
             * 1.根据topic和延迟队列id从consumeQueueTable查找需写入的ConsumeQueue
             *   如果未找到则新建（即ConsumeQueue文件是延迟创建的）。
             *  源码参考 ReputMessageService异步构建ConsumeQueue和IndexFile部分
             */
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                            delayLevel2QueueId(delayLevel));
            /**
             * 2.如果未找到对应的消息队列，新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，100ms后执行，本次任务结束
             */
            if (cq == null) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }
            /**
             * 3.根据逻辑offset定位到物理偏移量，然后截取该偏移量之后的一段Buffer（包含要拉取的消息的索引数据及对应consumeQueue文件之后的全部索引数据）。
             *   这里截取的Buffer可能包含多条索引数据，因为需批量拉取多条消息及进行消息过滤。
             *   源码参考在broker处理拉取消息请求部分。
             */
            SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
            //未获取到缓存buffer
            if (bufferCQ == null) {
                long resetOffset;
                // 如果当前消息队列的最小偏移量 大于 当前偏移量，则当前偏移量无效，设置新的offset为最小偏移量
                if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                            this.offset, resetOffset, cq.getQueueId());
                    // 如果当前消息队列的最大偏移量 小于 当前偏移量，则当前偏移量无效，设置新的offset为最大偏移量
                } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                            this.offset, resetOffset, cq.getQueueId());
                } else {
                    resetOffset = this.offset;
                }
                // 新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
                this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
                return;
            }
            /**
             * 3.遍历缓存buffer中的消息，根据tagsCode投递时间判断消息是否到期
             *   如果到期，则回复真实消息 并 投递到真实topic以及对应的queueId中
             */
            // 下一个消费点位
            long nextOffset = this.offset;
            try {
                // i：表示consumeQueue消息索引大小
                int i = 0;
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                // 遍历截取的Buffer中的consumeQueue消息索引，固定长度20b
                for (; i < bufferCQ.getSize() && isStarted(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    // 获取该条目对应的消息在commitlog文件中的物理偏移量
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    // 获取该条目对应的消息在commitlog文件中的总长度
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    /**
                     * 获取该条目对应的消息的tagsCode
                     * 对于延迟消息，tagsCode被替换为延迟消息的发送时间（CommitLog#checkMessageAndReturnSize方法中）
                     */
                    long tagsCode = bufferCQ.getByteBuffer().getLong();
                    // 如果tagsCode是扩展文件地址
                    if (cq.isExtAddr(tagsCode)) {
                        if (cq.getExt(tagsCode, cqExtUnit)) {
                            tagsCode = cqExtUnit.getTagsCode();
                        } else {
                            log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                    tagsCode, offsetPy, sizePy);
                            long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                            tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                        }
                    }
                    // 当前时间戳
                    long now = System.currentTimeMillis();
                    // 校验投递时间，必须小于等于当前时间 + 延迟时间
                    long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                    // 计算下一个offset
                    nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                    /**
                     * 如果投递时间大于当前时间
                     * 则新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
                     */
                    long countdown = deliverTimestamp - now;
                    if (countdown > 0) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                    // 根据消息物理偏移量从commitLog中找到该条消息。
                    MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
                    if (msgExt == null) {
                        continue;
                    }
                    // 恢复延迟消息
                    /**
                     * 构建内部消息对象，设置topic为REAL_TOPIC属性值（即原始topic），设置queueId为REAL_QID属性值（即原始queueId）。
                     * 即恢复为正常消息
                     */
                    MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                    if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                msgInner.getTopic(), msgInner);
                        continue;
                    }

                    boolean deliverSuc;
                    /**
                     * 消息投递
                     */
                    if (ScheduleMessageService.this.enableAsyncDeliver) {
                        // 异步投递，默认不支持
                        deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), nextOffset, offsetPy, sizePy);
                    } else {
                        // 默认同步投递
                        deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), nextOffset, offsetPy, sizePy);
                    }
                    // 如果投递失败，则新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
                    if (!deliverSuc) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                }
                // 遍历结束，更新下一个offset
                nextOffset = this.offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeup execute error, offset = {}", nextOffset, e);
            } finally {
                // 释放内存
                bufferCQ.release();
            }
            /**
             * 4.新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
             *   保证线程任务的活性
             */
            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        /**
         * 下一个调度任务
         * 新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，100ms后执行，本次任务结束
         * @param offset   消费偏移量
         * @param delay    延迟时间
         */
        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        /**
         * 同步投递
         * 第三个参数：当前偏移量
         * 之前是最开始的偏移量，在4.9.4版本已修复
         * @param msgInner  内部消息对象
         * @param msgId    消息id
         * @param offset   当前消费偏移量
         * @param offsetPy   消息物理偏移量
         * @param sizePy    消息大小
         * @return
         */
        private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
                                    int sizePy) {
            // 投递消息，内部调用asyncPutMessage方法
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
            // 投递结果
            PutMessageResult result = resultProcess.get();
            boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
            if (sendStatus) {
                // 如果发送成功，则更新offsetTable中的消费偏移量
                ScheduleMessageService.this.updateOffset(this.delayLevel, resultProcess.getNextOffset());
            }
            return sendStatus;
        }

        private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
                                     int sizePy) {
            Queue<PutResultProcess> processesQueue = ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            //Flow Control
            int currentPendingNum = processesQueue.size();
            int maxPendingLimit = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                    .getScheduleAsyncDeliverMaxPendingLimit();
            if (currentPendingNum > maxPendingLimit) {
                log.warn("Asynchronous deliver triggers flow control, " +
                        "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                return false;
            }

            //Blocked
            PutResultProcess firstProcess = processesQueue.peek();
            if (firstProcess != null && firstProcess.need2Blocked()) {
                log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
                return false;
            }

            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
            processesQueue.add(resultProcess);
            return true;
        }

        private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
                                                long offsetPy, int sizePy, boolean autoResend) {
            CompletableFuture<PutMessageResult> future =
                    ScheduleMessageService.this.writeMessageStore.asyncPutMessage(msgInner);
            return new PutResultProcess()
                    .setTopic(msgInner.getTopic())
                    .setDelayLevel(this.delayLevel)
                    .setOffset(offset)
                    .setPhysicOffset(offsetPy)
                    .setPhysicSize(sizePy)
                    .setMsgId(msgId)
                    .setAutoResend(autoResend)
                    .setFuture(future)
                    .thenProcess();
        }
    }

    public class HandlePutResultTask implements Runnable {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                    ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    switch (putResultProcess.getStatus()) {
                        case SUCCESS:
                            ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                            pendingQueue.remove();
                            break;
                        case RUNNING:
                            break;
                        case EXCEPTION:
                            if (!isStarted()) {
                                log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                                return;
                            }
                            log.warn("putResultProcess error, info={}", putResultProcess.toString());
                            putResultProcess.doResend();
                            break;
                        case SKIP:
                            log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                            pendingQueue.remove();
                            break;
                    }
                } catch (Exception e) {
                    log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                    putResultProcess.doResend();
                }
            }

            if (isStarted()) {
                ScheduleMessageService.this.handleExecutorService
                        .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * 延迟/重试消息写入消息结果
     */
    public class PutResultProcess {
        /**
         * topic
         */
        private String topic;
        /**
         * 当前消费偏移量
         */
        private long offset;
        /**
         * 消息物理偏移量
         */
        private long physicOffset;
        /**
         * 消息总大小
         */
        private int physicSize;
        /**
         * 延迟级别
         */
        private int delayLevel;
        /**
         * 消息id
         */
        private String msgId;
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;

        private volatile int resendCount = 0;
        private volatile ProcessStatus status = ProcessStatus.RUNNING;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public int getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            this.future.thenAccept(result -> {
                this.handleResult(result);
            });

            this.future.exceptionally(e -> {
                log.error("ScheduleMessageService put message exceptionally, info: {}",
                        PutResultProcess.this.toString(), e);

                onException();
                return null;
            });
            return this;
        }

        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                log.warn("ScheduleMessageService put message failed. info: {}.", result);
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.status = ProcessStatus.SUCCESS;
            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(result.getAppendMessageResult().getMsgNum());
            }
        }

        public void onException() {
            log.warn("ScheduleMessageService onException, info: {}", this.toString());
            if (this.autoResend) {
                this.status = ProcessStatus.EXCEPTION;
            } else {
                this.status = ProcessStatus.SKIP;
            }
        }

        public ProcessStatus getStatus() {
            return this.status;
        }

        public PutMessageResult get() {
            try {
                return this.future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        public void doResend() {
            log.info("Resend message, info: {}", this.toString());

            // Gradually increase the resend interval.
            try {
                Thread.sleep(Math.min(this.resendCount++ * 100, 60 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                    this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                    return;
                }

                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                PutMessageResult result = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);
                this.handleResult(result);
                if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    log.info("Resend message success, info: {}", this.toString());
                }
            } catch (Exception e) {
                this.status = ProcessStatus.EXCEPTION;
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        public boolean need2Blocked() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                    .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked;
        }

        public boolean need2Skip() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                    .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked * 2;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                    "topic='" + topic + '\'' +
                    ", offset=" + offset +
                    ", physicOffset=" + physicOffset +
                    ", physicSize=" + physicSize +
                    ", delayLevel=" + delayLevel +
                    ", msgId='" + msgId + '\'' +
                    ", autoResend=" + autoResend +
                    ", resendCount=" + resendCount +
                    ", status=" + status +
                    '}';
        }
    }

    public enum ProcessStatus {
        /**
         * In process, the processing result has not yet been returned.
         */
        RUNNING,

        /**
         * Put message success.
         */
        SUCCESS,

        /**
         * Put message exception. When autoResend is true, the message will be resend.
         */
        EXCEPTION,

        /**
         * Skip put message. When the message cannot be looked, the message will be skipped.
         */
        SKIP,
    }
}
