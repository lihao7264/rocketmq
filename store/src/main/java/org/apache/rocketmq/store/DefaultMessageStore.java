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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * RocketMQ的核心文件存储控制类
 * 负责消息存储文件
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * Broker的消息存储配置（比如：各种文件大小等）
     */
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;
    /**
     *  <topic,<queueId,ConsumeQueue（消费队列）>>： topic的ConsumeQueue的对应关系
     */
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    // ConsumeQueue文件的刷盘服务
    private final FlushConsumeQueueService flushConsumeQueueService;
    // 清除过期CommitLog文件的服务
    private final CleanCommitLogService cleanCommitLogService;
    // 清除过期ConsumeQueue文件的服务
    private final CleanConsumeQueueService cleanConsumeQueueService;

    /**
     * index索引文件服务
     */
    private final IndexService indexService;

    /**
     * MappedFile文件服务线程： 用于初始化MappedFile和预热MappedFile
     */
    private final AllocateMappedFileService allocateMappedFileService;

    // 根据CommitLog文件更新index文件索引和ConsumeQueue文件偏移量的服务
    private final ReputMessageService reputMessageService;

    // 高可用服务
    private final HAService haService;
    /**
     * 处理RocketMQ延迟消息的服务
     */
    private final ScheduleMessageService scheduleMessageService;
    // 存储一些统计指标信息的服务
    private final StoreStatsService storeStatsService;
    // 初始化MappedFile时，进行ByteBuffer的分配回收
    private final TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();
    /**
     * 系统锁（用于）
     */
    private final SystemClock systemClock = new SystemClock();

    /**
     * 存储定时任务线程池：单线程
     */
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    /**
     * broker状态管理器：保存Broker运行时状态，统计工作
     */
    private final BrokerStatsManager brokerStatsManager;
    /**
     * 消息送达的监听器：生产者消息到达时通过该监听器触发pullRequestHoldService通知pullRequestHoldService
     */
    private final MessageArrivingListener messageArrivingListener;
    /**
     * Broker的配置类：包含Broker的各种配置（比如：ROCKETMQ_HOME）
     */
    private final BrokerConfig brokerConfig;

    /**
     * 是否停止
     */
    private volatile boolean shutdown = true;

    /**
     * checkpoint 检查点文件，文件位置是{storePathRootDir}/checkpoint
     */
    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final AtomicInteger lmqConsumeQueueNum = new AtomicInteger(0);
    /**
     * 消息重放请求分发列表：
     * 可用于更新IndexFile、更新ConsumeQueue等信息
     * 默认有三个CommitLogDispatcher：
     *    CommitLogDispatcherBuildConsumeQueue：根据DispatchRequest写ConsumeQueue文件，构建ConsumeQueue索引。
     *    CommitLogDispatcherBuildIndex：根据DispatchRequest写IndexFile文件，构建IndexFile索引。
     *    CommitLogDispatcherCalcBitMap：根据DispatchRequest构建布隆过滤器，加速SQL92过滤效率，避免每次都解析sql。
     */
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    /**
     * 创建lockfile文件，名为lock，权限是"读写"，这是一个锁文件，用于获取文件锁。
     * 文件锁作用：保证磁盘上的这些存储文件同时只能有一个Broker的messageStore来操作。
     */
    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    // 创建消息存储对象MessageStore
    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        // 消息送达的监听器：生产者消息到达时通过该监听器触发pullRequestHoldService通知pullRequestHoldService
        this.messageArrivingListener = messageArrivingListener;
        // Broker的配置类：包含Broker的各种配置（比如：ROCKETMQ_HOME）
        this.brokerConfig = brokerConfig;
        // Broker的消息存储配置（比如：各种文件大小等）
        this.messageStoreConfig = messageStoreConfig;
        // broker状态管理器：保存Broker运行时状态，统计工作
        this.brokerStatsManager = brokerStatsManager;
        // 创建 MappedFile文件服务：用于初始化MappedFile和预热MappedFile
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        // 实例化CommitLog：DLedgerCommitLog表示主持主从自动切换功能，默认是CommitLog类型
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            // 创建DLedgerCommitLog类型的CommitLog
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        // topic的ConsumeQueue的对应关系：<topic,<queueId,ConsumeQueue（消费队列）>>
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
        // ConsumeQueue文件的刷盘服务
        this.flushConsumeQueueService = new FlushConsumeQueueService();
        // 清除过期CommitLog文件的服务
        this.cleanCommitLogService = new CleanCommitLogService();
        // 清除过期ConsumeQueue文件的服务
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        // 存储一些统计指标信息的服务
        this.storeStatsService = new StoreStatsService();
        // IndexFile索引文件服务
        this.indexService = new IndexService(this);
        // 高可用服务，默认为null
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }
        // 根据CommitLog文件更新index文件索引和ConsumeQueue文件偏移量的服务
        this.reputMessageService = new ReputMessageService();
        // 处理RocketMQ延迟消息的服务
        this.scheduleMessageService = new ScheduleMessageService(this);
        // 初始化MappedFile时，进行ByteBuffer的分配回收
        this.transientStorePool = new TransientStorePool(messageStoreConfig);
        // 如果当前节点不是从节点 且 是异步刷盘策略 且 transientStorePoolEnable参数配置为true，则启动该服务
        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }
        // 启动MappedFile文件服务线程
        this.allocateMappedFileService.start();
        // 启动index索引文件服务线程
        this.indexService.start();
        // 转发服务列表：监听CommitLog文件中的新消息存储，再调用列表中的CommitLogDispatcher#dispatch方法
        this.dispatcherList = new LinkedList<>();
        // 通知ConsumeQueue的Dispatcher，可用于更新ConsumeQueue的偏移量等信息
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        // 通知IndexFile的Dispatcher，可用于更新IndexFile的时间戳等信息
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());
        // 获取锁文件，路径就是配置的{storePathRootDir}/lock
        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        // 确保创建file文件的父目录（即{storePathRootDir}目录）
        MappedFile.ensureDirOK(file.getParent());
        // 确保创建commitlog目录（即{StorePathCommitLog}目录）
        MappedFile.ensureDirOK(getStorePathPhysic());
        // 确保创建consumequeue目录（即{storePathRootDir}/consumequeue目录）
        MappedFile.ensureDirOK(getStorePathLogic());
        /**
         * 创建lockfile文件，名为lock，权限是"读写"，这是一个锁文件，用于获取文件锁。
         * 文件锁作用：保证磁盘上的这些存储文件同时只能有一个Broker的messageStore来操作。
         */
        lockFile = new RandomAccessFile(file, "rw");
    }

    /**
     * 截断无效consumequeue文件
     * @param phyOffset commitlog文件的最大有效区域的偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffset) {
        // 获取consumeQueueTable
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
        // 遍历
        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // 对每一个consumequeue文件的数据进行校验，可能会删除consumequeue文件 或 更新相关属性
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 加载恢复消息文件
     * 通过消息存储服务加载消息存储的相关文件（ broker启动的核心步骤之一）
     * 举例：commitLog日志文件、consumequeue消息消费队列文件的加载，indexFile索引文件的构建
     * messageStore会将这些文件（CommitLog、ConsumeQueue、IndexFile等文件）的内容加载到内存中，并完成RocketMQ的数据恢复
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            /**
             * 1.判断上次broker是否是正常退出。
             *    如果是正常退出，则不会保留abort文件。
             *    如果是异常退出，则会保留abort文件。
             * Broker在启动时会创建{storePathRootDir}/abort文件，并注册钩子函数：在JVM退出时删除abort文件。
             * 如果下一次启动时存在abort文件，说明Broker是异常退出的，文件数据可能不一致，需进行数据修复。
             */
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");
            /**
             * 2.加载CommitLog日志文件，目录路径取自broker.conf文件中的storePathCommitLog属性
             *   CommitLog文件是真正存储消息内容的地方，单个文件默认大小1G。
             */
            // load Commit Log
            result = result && this.commitLog.load();
            /**
             * 3.加载ConsumeQueue文件，目录路径为{storePathRootDir}/consumequeue，文件组织方式为topic/queueId/fileName
             *      ConsumeQueue文件可看作是CommitLog的索引文件，存储了它所属Topic的消息在CommitLog中的偏移量
             *      消费者拉取消息时，可以从ConsumeQueue中快速的根据偏移量定位消息在CommitLog中的位置。
             */
            // load Consume Queue
            result = result && this.loadConsumeQueue();

            if (result) {
                /**
                 * 4.加载checkpoint 检查点文件，文件位置是{storePathRootDir}/checkpoint
                 *   StoreCheckpoint记录着commitLog、ConsumeQueue、Index文件的最后更新时间点，
                 *   当上一次broker是异常结束时，会根据StoreCheckpoint的数据进行恢复，这决定着文件从哪里开始恢复，甚至是删除文件
                 */
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
                /**
                 * 5.加载 index 索引文件，目录路径为{storePathRootDir}/index
                 *   index 索引文件作用：用于通过时间区间来快速查询消息，底层为HashMap结构，实现为hash索引。
                 *   如果时非正常退出，且最大更新时间戳比checkpoint文件中的时间戳大，则删除该 index 文件
                 */
                this.indexService.load(lastExitOK);
                /**
                 * 6.恢复ConsumeQueue文件和CommitLog文件，将正确的的数据恢复至内存中，删除错误数据和文件。
                 */
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
                /**
                 * 7.加载RocketMQ延迟消息的服务，包括延时等级、配置文件等等。
                 */
                if (null != scheduleMessageService) {
                    result =  this.scheduleMessageService.load();
                }
            }

        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            // 如果上面的操作抛出异常，则文件服务停止
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             */
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                    maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
            // 启动消息重放服务
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            this.recoverTopicQueueTable();
        }

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        this.createTempFile();
        // 启动定时任务
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.diskCheckScheduledExecutorService.shutdown();
            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    /**
     * 检查消息
     * @param msg
     * @return
     */
    private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
        // 如果topic长度大于127，则返回MESSAGE_ILLEGAL（表示topic过长）
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        // 如果设置的属性长度大于32767，则返回MESSAGE_ILLEGAL（表示properties过长）
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + messageExtBatch.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        return PutMessageStatus.PUT_OK;
    }

    /**
     * 检查消息存储状态（即是否支持写入消息）
     * @return
     */
    private PutMessageStatus checkStoreStatus() {
        // 如果DefaultMessageStore是shutdown状态，返回SERVICE_NOT_AVAILABLE
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }
        // 如果broker是SLAVE角色，则返回SERVICE_NOT_AVAILABLE，不能将消息写入SLAVE角色
        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("broke role is slave, so putMessage is forbidden");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }
        /**
         * 如果不支持写入，则返回SERVICE_NOT_AVAILABLE
         * 可能因为broker磁盘已满、写入逻辑队列错误、写入索引文件错误等原因
         */
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("the message store is not writable. It may be caused by one of the following reasons: " +
                        "the broker's disk is full, write to logic queue error, write to index file error, etc");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        } else {
            this.printTimes.set(0);
        }
        /**
         * 如果操作系统页缓存繁忙，则返回OS_PAGECACHE_BUSY
         *  如果broker持有锁的时间超过osPageCacheBusyTimeOutMills，则算作操作系统页缓存繁忙
         */
        if (this.isOSPageCacheBusy()) {
            return PutMessageStatus.OS_PAGECACHE_BUSY;
        }
        // 返回PUT_OK，表示可以存储消息
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkLmqMessage(MessageExtBrokerInner msg) {
        if (msg.getProperties() != null
                && StringUtils.isNotBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
                && this.isLmqConsumeQueueNumExceeded()) {
            return PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED;
        }
        return PutMessageStatus.PUT_OK;
    }

    private boolean isLmqConsumeQueueNumExceeded() {
        if (this.getMessageStoreConfig().isEnableLmq() && this.getMessageStoreConfig().isEnableMultiDispatch()
                && this.lmqConsumeQueueNum.get() > this.messageStoreConfig.getMaxLmqConsumeQueueNum()) {
            return true;
        }
        return false;
    }

    /**
     * 处理、存储消息
     *
     * @param msg 需存储的MessageInstance
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        /**
         * 1.检查存储状态
         */
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        // 如果不是PUT_OK，则直接返回
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }
        /**
         * 2.检查消息
         */
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }
        /**
         * 2.检查 light message queue(LMQ)（即微消息队列）
         */
        PutMessageStatus lmqMsgCheckStatus = this.checkLmqMessage(msg);
        if (msgCheckStatus == PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED) {
            return CompletableFuture.completedFuture(new PutMessageResult(lmqMsgCheckStatus, null));
        }
        // 当前时间戳
        long beginTime = this.getSystemClock().now();
        /**
         * 核心方法，调用CommitLog#asyncPutMessage方法存储消息
         */
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept(result -> {
            // 存储消息消耗的时间
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            // 更新统计保存消息花费的时间和最大花费的时间
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                // 如果存储失败，则增加保存消息失败的次数
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> resultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        resultFuture.thenAccept(result -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
            }

            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return resultFuture;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 调用asyncPutMessage方法异步发送消息
        return waitForPutResult(asyncPutMessage(msg));
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return waitForPutResult(asyncPutMessages(messageExtBatch));
    }

    /**
     * 调用putMessageResultFuture#get方法同步等待结果
     * 等待时间： 同步刷盘超时时间（默认值：5s）  和 主从同步超时时间（默认值：3s）+5s中的最大值
     *   默认等待时间：3s+5s=8s
     * @param putMessageResultFuture
     * @return
     */
    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                    Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                            this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            log.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                    + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                    + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    /**
     * 操作系统页缓存是否繁忙
     * @return
     */
    @Override
    public boolean isOSPageCacheBusy() {
        /**
         *  一个broker将所有的消息都追加到同一个逻辑CommitLog日志文件中，因此需通过获取putMessageLock锁来控制并发。
         *   begin：表示获取CommitLog锁的开始时间
         */
        long begin = this.getCommitLog().getBeginTimeInLock();
        // 计算锁的持有时间，当前时间减去获取锁开始时间，该时间可看作是处理上一个消息目前所花费的时间
        long diff = this.systemClock.now() - begin;

        /**
         *  如果broker持有锁的时间超过osPageCacheBusyTimeOutMills，则算作操作系统页缓存繁忙，会拒绝处理当前请求
         *  直观现象：客户端抛出[REJECTREQUEST]system busy, start flow control for a while异常
         *  osPageCacheBusyTimeOutMills可配置（默认值：1000ms，即1s）
         */
        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 批量获取消息
     * 从给定偏移量开始，在queueId中最多查询属于topic的最多maxMsgNums条消息
     * 获取的消息将使用提供的消息过滤器（messageFilter）进行进一步筛选
     * @param group   所属消费者组
     * @param topic   查询的topic
     * @param queueId  查询的queueId
     * @param offset   起始逻辑偏移量
     * @param maxMsgNums   要查询的最大消息数（默认：32）
     * @param messageFilter 用于筛选所需消息的消息过滤器
     * @return  匹配的消息
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        /**
         * 1.前置校验
         */
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        if (MixAll.isLmq(topic) && this.isLmqConsumeQueueNumExceeded()) {
            log.warn("message store is not available, broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num");
            return null;
        }
        // 起始时间
        long beginTime = this.getSystemClock().now();
        /**
         * 拉取消息的状态
         * 默认：队列中没有消息
         */
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        // 下次拉取的consumeQueue的起始逻辑偏移量
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        // lazy init when find msg.
        GetMessageResult getResult = null;
        // 获取commitLog的最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();
        /**
         * 根据topic和队列id确定需写入的ConsumeQueue
         */
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            /**
             * 2.偏移量校验
             */
            // 获取consumeQueue的minOffset（最小逻辑偏移量offset）、maxOffset（最打逻辑偏移量offset）
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                // maxOffset（最大的逻辑偏移量offset）为0，表示消息队列无消息，设置NO_MESSAGE_IN_QUEUE
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                /**
                 * 校正下一次拉取的开始偏移量
                 * 如果broker不是SLAVE节点 或 SLAVE节点+从服务器支持offset检查，则为0
                 */
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                // consumer传递的offset小于最小偏移量，则表示拉取的位置太小，设置OFFSET_TOO_SMALL
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                /**
                 * 校正下一次拉取的开始偏移量
                 * 如果broker不是SLAVE节点 或 SLAVE节点+从服务器支持offset检查，则为minOffset
                 */
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                // consumer传递的offset等于最大偏移量，表示拉取的位置溢出，设置OFFSET_OVERFLOW_ONE
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                /**
                 *  校正下一次拉取的开始偏移量，还是offset
                 */
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                // consumer传递的offset大于最大偏移量，表示拉取的位置严重溢出，设置OFFSET_OVERFLOW_BADLY
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                /**
                 * 校正下一次拉取的开始偏移量
                 * 如果broker不是SLAVE节点 或 SLAVE节点+从服务器支持offset检查，则为maxOffset
                 */
                nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
            } else {
                // consumer传递的offset大于等于minOffset 且 小于maxOffset，表示偏移量在正常范围内
                /**
                 * 3.根据逻辑offset定位到物理偏移量，截取该偏移量之后的一段Buffer（包含要拉取的消息的索引数据 及 对应consumeQueue文件之后的全部索引数据）。
                 *   一条consumeQueue索引的默认固定长度：20B
                 *   这里截取的Buffer可能包含多条索引数据，但一定包含将要拉取的下一条数据。
                 */
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                // 如果截取到了缓存区数据，那么从Buffer中检查索引数据 及 查找commitLog中的消息数据
                if (bufferConsumeQueue != null) {
                    try {
                        // 先设置为NO_MATCHED_MESSAGE
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;
                        /**
                         * 下一个commitLog文件的起始物理偏移量，默认从Long.MIN_VALUE开始
                         * 用于记录上一次循环时，是否换到了下一个commitLog文件
                         */
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        // 本次消息拉取的最大物理偏移量
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        // 每次最大的过滤消息字节数，一般为16000/20 = 800 条
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        /**
                         * 是否需记录commitLog磁盘的剩余可拉取的消息字节数
                         * 默认值：true
                         */
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        // 创建拉取结果对象
                        getResult = new GetMessageResult(maxMsgNums);
                        // 存储单元
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        /**
                         * 4.循环遍历截取的buffer，处理每一条ConsumeQueue索引，拉取消息
                         *   ConsumeQueue消息固定长度20字节，因此每次移动20B的长度
                         */
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 消息在CommitLog中的物理偏移量
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            // 消息大小
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            /**
                             * 延迟消息时，tag是消息投递时间
                             * 其它消息时，tag时 消息的tags的hashCode（即生产者发送消息时设置的tags）
                             * 消费数据时，可通过对比消费者设置的过滤信息来匹配消息
                             */
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();
                            // 更新maxPhyOffsetPulling为当前消息在CommitLog中的物理偏移量
                            maxPhyOffsetPulling = offsetPy;

                            /**
                             * 如果nextPhyFileStartOffset不为Long.MIN_VALUE 且 offsetPy 小于 nextPhyFileStartOffset
                             * 表示切换到了下一个commitLog文件 且 当前偏移量 小于 该文件最小偏移量，则跳过该消息的处理
                             */
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }
                            /**
                             * 4.1 检查要拉取的消息是否在磁盘上
                             */
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                            /**
                             * 4.2 判断消息拉取是否达到上限，如果达到上限，则跳出循环，结束消息的拉取
                             */
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) {
                                break;
                            }
                            // 额外信息判断，一般没有
                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }
                            /**
                             * 4.3 通过messageFilter#isMatchedByConsumeQueue方法执行消息tagsCode过滤
                             *     tagsCode在ConsumeQueue中保存着，因此基于ConsumeQueue条目就能执行broker端的TAG过滤
                             */
                            if (messageFilter != null
                                    && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                // 如果过滤未通过 且 已拉取的消息总大小为0，则设置为NO_MATCHED_MESSAGE状态
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // 跳过该索引条目，拉取下一个索引条目
                                continue;
                            }
                            /**
                             * 4.4 TAG校验通过，调用commitLog#getMessage方法根据消息的物理偏移量和消息大小获取该索引对应的真正的消息
                             *    索引中包含了消息的物理偏移量和消息大小，因此能从commitLog中获取真正的消息所在的内存，而消息的格式是固定的，因此能解析出里面的数据
                             */
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            // 未找到消息，表示该偏移量可能到达了文件末尾，消息存放在下一个commitLog文件中
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }
                                // nextPhyFileStartOffset设置为下一个commitLog文件的起始物理偏移量 并 跳过本次拉取
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }
                            /**
                             * 4.5 找到了消息，通过messageFilter#isMatchedByCommitLog方法执行消息SQL92 过滤
                             * SQL92 过滤依赖于消息中的属性，而消息体的内容存放在commitLog中的，因此需先拉取到消息，再进行SQL92过滤
                             */
                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                //  release...
                                // 过滤不通过，释放这一段内存 并 跳过本次拉取
                                selectResult.release();
                                continue;
                            }
                            // 传输的单条消息数自增1，用于控制台展示
                            this.storeStatsService.getGetMessageTransferedMsgCount().add(1);
                            /**
                             * 4.6 TAG和SQL92校验通过，则将消息存入getResult，注意：存入的是一段
                             */
                            getResult.addMessage(selectResult);
                            // 更改status
                            status = GetMessageStatus.FOUND;
                            // nextPhyFileStartOffset重新置为Long.MIN_VALUE，继续下一次循环
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                        /**
                         * 如果需记录commitLog磁盘的剩余可拉取的消息字节数
                         * 默认值：true
                         */
                        if (diskFallRecorded) {
                            // 剩余的commitLog磁盘可拉取的消息字节数 = 磁盘最大物理偏移量 - 本次消息拉取的最大物理偏移量
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            // 记录剩余的commitLog磁盘可拉取的消息字节数
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }
                        /*
                         * 5.计算下一次读取数据的ConsumeQueue开始偏移量，判断是否建议下一次从SLAVE broker中拉取消息
                         */
                        // 计算下一次读取数据的ConsumeQueue的开始偏移量
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        // 剩余的commitLog磁盘可拉取的消息字节数=磁盘最大物理偏移量 - 本次消息拉取的最大物理偏移量
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        // broker服务最大可使用物理内存
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        // 如果剩余的commitLog磁盘可拉取的消息字节数 大于 broker服务最大可使用物理内存，则设置建议下一次从SLAVE broker中拉取消息
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {
                        // 截取的buffer内存释放
                        bufferConsumeQueue.release();
                    }
                } else {
                    // 未获取到缓存buffer，可能是到达了当前consumeQueue文件的尾部
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    // nextBeginOffset设置为consumeQueue的下一个文件的起始偏移量
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            // 未找到consumeQueue
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            // nextBeginOffset设置为0
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            /**
             * 找到了消息，则拉取到的次数统计字段getMessageTimesTotalFound+1，用于控制台展示
             * broker中的tps只计算拉取次数，而非拉取的消息条数，默认情况下pushConsumer一次拉取32条
             */
            this.storeStatsService.getGetMessageTimesTotalFound().add(1);
        } else {
            // 未找到消息，则未拉取到的次数统计字段 getMessageTimesTotalMiss+1，用于控制台展示
            this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
        }
        // 计算本次拉取消耗的时间
        long elapsedTime = this.getSystemClock().now() - beginTime;
        // 尝试比较并更新最长的拉取消息的时间字段 getMessageEntireTimeMax
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // lazy init no data found.
        if (getResult == null) {
            // 未找到消息的情况，延迟初始化GetMessageResult，设置拉取结果为0
            getResult = new GetMessageResult(0);
        }
        /**
         * 6.设置getResult的属性并返回
         */
        // 设置拉取状态
        getResult.setStatus(status);
        // 设置下次拉取的consumeQueue的起始逻辑偏移量
        getResult.setNextBeginOffset(nextBeginOffset);
        // 设置consumeQueue的最小、最大的逻辑偏移量offset
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    /**
     * DefaultMessageStore#getStorePathPhysic()
     * 获取commitLog文件存储路径
     * @return
     */
    public String getStorePathPhysic() {
        String storePathPhysic;
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            // 如果启用了 DLedger
            storePathPhysic = ((DLedgerCommitLog)DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            // 如果未启用了 DLedger
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }
    /**
     * DefaultMessageStore#getStorePathLogic()
     * consumequeue文件存储路径
     */
    public String getStorePathLogic() {
        // 默认路径：{storePathRootDir}/consumequeue
        return StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double minPhysicsUsedRatio = Double.MAX_VALUE;
            String commitLogStorePath = getStorePathPhysic();
            String[] paths = commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            for (String clPath : paths) {
                double physicRatio = UtilAll.isPathExists(clPath) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(clPath) : -1;
                result.put(RunningStats.commitLogDiskRatio.name() + "_" + clPath, String.valueOf(physicRatio));
                minPhysicsUsedRatio = Math.min(minPhysicsUsedRatio, physicRatio);
            }
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(minPhysicsUsedRatio));
        }

        {
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathLogic());
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        long minPhyOffset = this.getMinPhyOffset();
        if (this.getCommitLog() instanceof DLedgerCommitLog) {
            minPhyOffset += DLedgerEntry.BODY_OFFSET;
        }
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.executeDeleteFilesManually();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    /**
     * 主从节点的差异
     * @return
     */
    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)
                    && !MixAll.isLmq(topic)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 根据topic和队列id查找ConsumeQueue
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        // 从consumeQueueTable中获取该topic所有的队列
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        // 如果未保存该topic，则存入一个空的map
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }
        // 从map中根据queueId 获取对应的 消费队列
        ConsumeQueue logic = map.get(queueId);
        // 如果ConsumeQueue为null，则新建，所以ConsumeQueue是延迟创建的
        if (null == logic) {
            // 新建ConsumeQueue
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    // {$ROCKETMQ_HOME}/store/consumequeue
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    // 单个文件大小，默认为可存储30W数据的大小，每条数据20Byte
                    this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                    this);
            // 存入map中，如果已存在则取旧的
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                // light message queue(LMQ)
                if (MixAll.isLmq(topic)) {
                    lmqConsumeQueueNum.getAndIncrement();
                }
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 校正下一次拉取偏移量
     * @param oldOffset  consumer传递的offset
     * @param newOffset  broker设置的offset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        // 设置为consumer传递的offset
        long nextOffset = oldOffset;
        // 如果broker不是SLAVE节点 或 SLAVE节点+从服务器支持offset检查
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            // 设置为新的offset
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * 检查消息是否在磁盘上
     * @param offsetPy    拉取的消息在commitLog中的物理偏移量
     * @param maxOffsetPy commitLog中的最大物理偏移量
     * @return  是否在磁盘中
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        // 获取broker最大可用内存（默认值：机器物理最大可用内存 * 40/100 ， 即broker最大可用内存为最大物理内存的百分之40）
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        // 如果commitLog中的最大物理偏移量 - 拉取的消息在commitLog中的物理偏移量 的差值大于获取broker最大可用内存，则任务数据已在磁盘上，否则认为还在内存中
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 判断是否达到了消息拉取的上限
     * @param sizePy      本次消息字节大小
     * @param maxMsgNums   要查询的最大消息数（默认值：32）
     * @param bufferTotal  已拉取的消息总大小
     * @param messageTotal   已拉取的消息总数
     * @param isInDisk      本次消息是否在磁盘上
     * @return  是否达到了消息拉取的上限
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        /**
         *  如果已拉取的消息总大小 或 已拉取的消息总数是0，则返回false（表示还未拉取到消息）
         */
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        /**
         * 如果要查询的最大消息数（默认值：32） 小于等于 已拉取的消息总数，则返回true
         * 表示拉取数量达到了阈值
         */
        if (maxMsgNums <= messageTotal) {
            return true;
        }
        // 如果当前消息在磁盘中
        if (isInDisk) {
            /**
             * 如果已拉取消息字节数 + 待拉取的当前消息的字节大小 大于 maxTransferBytesOnMessageInDisk = 64KB ，则返回true
             * 表示从磁盘上拉取消息的大小超过了阈值64KB
             */
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            /**
             *  如果已拉取的消息总数 > maxTransferCountOnMessageInDisk - 1= 8 - 1，则返回true
             *  表示从磁盘上拉取消息的数量超过了阈值8条
             */
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
            // 如果当前消息在内存中
        } else {
            /**
             * 如果已拉取消息字节数 + 待拉取的当前消息的字节大小 大于 maxTransferBytesOnMessageInMemory = 256KB ，则返回true
             * 表示从内存中拉取消息的大小超过了阈值256KB
             */
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            /**
             * 如果已拉取的消息总数 > maxTransferCountOnMessageInMemory - 1= 32 - 1，则返回true
             * 表示从内存中拉取消息的数量超过了阈值32条
             */
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 启动定时任务
     */
    private void addScheduleTask() {
        // 1、启动定时清理过期文件线程：
        //    初始延迟：60s。
        //    第一次后，默认每 10s 执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // 定期清理文件
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);
        // 10分钟 检查映射文件队列（CommitLog、）中的文件大小是否正确
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
        //
        this.diskCheckScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
            }
        }, 1000L, 10000L, TimeUnit.MILLISECONDS);
    }

    /**
     * DefaultMessageStore#cleanFilesPeriodically()
     * 定期清理文件（CommitLog、ConsumeQueue、IndexFile）
     */
    private void cleanFilesPeriodically() {
        // 1、调用CommitLog 的清理方法
        this.cleanCommitLogService.run();
        // 2、调用  ConsumeQueue、IndexFile的清理方法
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    /**
     * 是否存在临时文件（abort文件）
     * @return
     */
    private boolean isTempFileExist() {
        // 获取临时文件路径：{storePathRootDir}/abort（{ROCKET_HOME}/store/abort）
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        // 构建file文件对象
        File file = new File(fileName);
        // 判断文件是否存在
        return file.exists();
    }

    /**
     * 加载ConsumeQueue文件，目录路径为{storePathRootDir}/consumequeue，文件组织方式为topic/queueId/fileName
     *      ConsumeQueue文件可看作是CommitLog的索引文件，存储了它所属Topic的消息在CommitLog中的偏移量
     *      消费者拉取消息时，可以从ConsumeQueue中快速的根据偏移量定位消息在CommitLog中的位置。
     */
    private boolean loadConsumeQueue() {
        // 获取ConsumeQueue文件所在目录，目录路径为{storePathRootDir}/consumequeue
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        // 获取目录下文件列表（即topic目录列表）
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            // 遍历topic目录
            for (File fileTopic : fileTopicList) {
                // 获取topic名字
                String topic = fileTopic.getName();
                // 获取topic目录下的队列id目录
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            // 获取队列id
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        /**
                         * 创建ConsumeQueue对象，一个队列id目录对应着一个ConsumeQueue对象
                         */
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                // ConsumeQueue文件大小默认值：30w数据
                                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                                this);
                        // 将ConsumeQueue对象及其对应关系存入consumeQueueTable中
                        this.putConsumeQueue(topic, queueId, logic);
                        // 加载ConsumeQueue文件
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }
    /**
     * 恢复ConsumeQueue文件和CommitLog文件，将正确的的数据恢复至内存中，删除错误数据和文件。
     */
    private void recover(final boolean lastExitOK) {
        // 恢复所有的ConsumeQueue文件，返回在ConsumeQueue存储的最大有效commitlog偏移量
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        if (lastExitOK) {
            // 正常恢复commitLog
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            // 异常恢复commitLog
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }
        // 最后恢复topicQueueTable
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
            if (MixAll.isLmq(topic)) {
                this.lmqConsumeQueueNum.getAndIncrement();
            }
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 恢复所有的ConsumeQueue文件，返回在ConsumeQueue有效区域存储的最大的commitlog偏移量
     * @return
     */
    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        // 遍历consumeQueueTable的value集合（即queueId到ConsumeQueue的map映射）
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            // 遍历所有的ConsumeQueue
            for (ConsumeQueue logic : maps.values()) {
                // 恢复ConsumeQueue，删除无效ConsumeQueue文件
                logic.recover();
                // 如果当前queueId目录下的所有ConsumeQueue文件的最大有效物理偏移量 大于 此前记录的最大有效物理偏移量
                //  则更新记录的ConsumeQueue文件的最大commitlog有效物理偏移量
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        // 返回ConsumeQueue文件的最大commitlog有效物理偏移量
        return maxPhysicOffset;
    }

    /**
     * 恢复consumeQueueTable
     */
    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        // 获取CommitLog的最小偏移量
        long minPhyOffset = this.commitLog.getMinOffset();
        // 遍历consumeQueueTable（即ConsumeQueue文件的集合）
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                // 将“topicName-queueId”作为key，将当前queueId下最大的相对偏移量作为value存入table
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }
        // 设置为topicQueueTable
        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * @param req 分发请求
     */
    public void doDispatch(DispatchRequest req) {
        // 循环调用CommitLogDispatcher#dispatch处理
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * 写入消息位置信息
     * @param dispatchRequest  分派消息请求
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        /**
         * 根据topic和队列id确定ConsumeQueue
         */
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        /**
         * 将消息信息追加到ConsumeQueue索引文件中
         */
        cq.putMessagePositionInfoWrapper(dispatchRequest, checkMultiDispatchQueue(dispatchRequest));
    }

    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStoreConfig.isEnableMultiDispatch()) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    @Override
    public void cleanUnusedLmqTopic(String topic) {
        if (this.consumeQueueTable.containsKey(topic)) {
            ConcurrentMap<Integer, ConsumeQueue> map = this.consumeQueueTable.get(topic);
            if (map != null) {
                ConsumeQueue cq = map.get(0);
                cq.destroy();
                log.info("cleanUnusedLmqTopic: {} {} ConsumeQueue cleaned",
                        cq.getTopic(),
                        cq.getQueueId()
                );

                this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                this.lmqConsumeQueueNum.getAndDecrement();
            }
            this.consumeQueueTable.remove(topic);
            if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                this.brokerStatsManager.onTopicDeleted(topic);
            }
            log.info("cleanUnusedLmqTopic: {},topic destroyed", topic);
        }
    }

    public int remainTransientStoreBufferNumbs() {
        // 检查可用buffers
        return this.transientStorePool.availableBufferNums();
    }

    /**
     * 检查临时存储池是否不足
     * @return
     */
    @Override
    public boolean isTransientStorePoolDeficient() {
        // 如果堆外内存池个数为0，则表示临时存储池是否不足
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * 通知ConsumeQueue的Dispatcher：用于接收分发请求并构建ConsumeQueue
     *   更新ConsumeQueue的偏移量等信息
     *
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        /**
         * @param request 分派消息请求
         */
        @Override
        public void dispatch(DispatchRequest request) {
            // 从该消息的消息系统flag中获取事务状态
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                // 如果不是事务消息 或 是事务commit消息，则进行处理
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // 写入消息位置信息到consumeQueue
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                // 如果是事务prepared消息 或 是事务rollback消息，则不进行处理
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    /**
     * 通知IndexFile的Dispatcher：用于接收分发请求并构建IndexFile
     *   可用于更新IndexFile的时间戳等信息
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        /**
         * 写入消息位置信息到IndexFile
         * @param request 分派消息请求
         */
        @Override
        public void dispatch(DispatchRequest request) {
            /**
             * 是否支持IndexFile
             * 默认值：true
             */
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                // 构建Index
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * 清除（清理）过期CommitLog文件的服务
     */
    class CleanCommitLogService {
        // 手动触发删除后，需删除的默认次数为20次
        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        // Broker拒绝消息写入的磁盘空间占用率，默认90%
        private final double diskSpaceWarningLevelRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));
        // 强制清理的磁盘空间占用率，默认85%
        private final double diskSpaceCleanForciblyRatio =
                Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        // 上一次重新删除的时间
        private long lastRedeleteTimestamp = 0;
        /**
         * 手动触发删除后，manualDeleteFileSeveralTimes为20，每执行一次删除方法减少一次
         */
        private volatile int manualDeleteFileSeveralTimes = 0;
        /**
         * 达到 强制清理的磁盘空间占用率 或 Broker拒绝消息写入的磁盘空间占用率，则为true
         */
        private volatile boolean cleanImmediately = false;

        /**
         * DefaultMessageStore.CleanCommitLogService#executeDeleteFilesManually()
         * 修改 手动触发文件删除后，需删除的次数为默认20次
         */
        public void executeDeleteFilesManually() {
            // 手动触发文件删除后，需删除的次数
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        /**
         * DefaultMessageStore.CleanCommitLogService#run()
         * 清理CommitLog服务的运行方法
         */
        public void run() {
            try {
                // 1、删除过期文件集合
                this.deleteExpiredFiles();
                // 2、删除可能存在的应被删除 但 未删除成功的文件
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * DefaultMessageStore.CleanCommitLogService#deleteExpiredFiles()
         * 删除文件集合
         */
        private void deleteExpiredFiles() {
            // 本次删除的文件数量
            int deleteCount = 0;
            // 文件保留时间，默认72h。
            // 如果超出该时间，则认为是过期文件，可被删除
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // 删除物理文件的时间间隔，默认100ms。
            // 在一次删除过程中，删除两个文件的间隔时间
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            /**
             * 第一次拒绝删除后，能保留文件的最大时间。
             * 默认120s。
             * 在删除文件时，如果该文件被其它线程占用，会阻止删除，同时在第一次试图删除该文件时记录当前时间戳。
             * 在保留时间内，文件可以拒绝删除，超过该时间后，会将引用次数设置为负数，文件将被强制删除。
             */
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            // 满足删除文件的时间（4点-5点时间窗）
            boolean timeup = this.isTimeToDelete();
            // 磁盘空间是否不足（75%）
            boolean spacefull = this.isSpaceToDelete();
            // 手动删除是否被触发（触发则设manualDeleteFileSeveralTimes为20，每执行一次删除方法减少一次）
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;
            /**
             * 满足下列条件之一将继续删除
             * 1. 到了设置的每天固定删除时间（4点-5点时间窗）
             * 2. 磁盘空间不充足，默认为75%
             * 3. executeDeleteFilesManually方法被调用，手工删除文件
             */
            if (timeup || spacefull || manualDelete) {
                // 手动删除文件次数-1
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;
                // 是否立即强制删除文件（磁盘空间大于85%（diskSpaceCleanForciblyRatio、diskSpaceWarningLevelRatio）为true）
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);
                // commitLog 文件保留时间（默认 72小时）
                fileReservedTime *= 60 * 60 * 1000;
                // 删除成功的文件数量
                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                    // 危险情况：磁盘满了，但又无法删除文件
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * DefaultMessageStore.CleanCommitLogService#redeleteHangedFile()
         * 删除可能存在的应被删除 但 未删除成功的文件
         */
        private void redeleteHangedFile() {
            // 1、获取重新删除间隔（默认120s）
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            // 2、当前时间-上一次重试时间戳> redeleteHangedFileInterval
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                // 更新上一次重新删除的时间为当前时间
                this.lastRedeleteTimestamp = currentTimestamp;
                // 删除的文件被引用时，不会马上被删除，最大的存活时间（默认120s）
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                // 重试删除第一个文件
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 是否满足删除文件的时间（默认4点-5点时间窗）
         * DefaultMessageStore.CleanCommitLogService#isTimeToDelete()
         * @return
         */
        private boolean isTimeToDelete() {
            // 文件过期后，定时清理的时间点
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            // 是否在该时间内
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         *  DefaultMessageStore.CleanCommitLogService#isSpaceToDelete()
         *  磁盘空间是否不足（默认75%）
         * @return
         */
        private boolean isSpaceToDelete() {
            // 获取 过期清理警戒线的磁盘空间占用率
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            // 是否强制清理存储文件
            cleanImmediately = false;

            {   // 获取commitLog文件存储路径
                String commitLogStorePath = DefaultMessageStore.this.getStorePathPhysic();
                // 多个存储路径
                String[] storePaths = commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                // 达到强制清理的磁盘空间占用率的磁盘路径
                Set<String> fullStorePath = new HashSet<>();
                // 最小的磁盘占用率
                double minPhysicRatio = 100;
                // 最小的磁盘路径
                String minStorePath = null;
                for (String storePathPhysic : storePaths) {
                    // 获取磁盘占用率
                    double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                    if (minPhysicRatio > physicRatio) {
                        minPhysicRatio =  physicRatio;
                        minStorePath = storePathPhysic;
                    }

                    if (physicRatio > diskSpaceCleanForciblyRatio) {
                        fullStorePath.add(storePathPhysic);
                    }
                }
                DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
                /**
                 * 最小的磁盘占用率 达到 Broker拒绝消息写入的磁盘空间占用率，强制清除存储文件 并 设置磁盘满标记（停止写入）
                 */
                if (minPhysicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        // 磁盘从不满到满的时候，才会打印该日志
                        DefaultMessageStore.log.error("physic disk maybe full soon " + minPhysicRatio +
                                ", so mark disk full, storePathPhysic=" + minStorePath);
                    }

                    cleanImmediately = true;
                } else if (minPhysicRatio > diskSpaceCleanForciblyRatio) {
                    /**
                     * 最小的磁盘占用率 达到 强制清理的磁盘空间占用率，则强制清除存储文件
                     */
                    cleanImmediately = true;
                } else {
                    /**
                     * 设置磁盘为未满
                     */
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        // 磁盘满过后，再不满才会打印该日志
                        DefaultMessageStore.log.info("physic disk space OK " + minPhysicRatio +
                                ", so mark disk ok, storePathPhysic=" + minStorePath);
                    }
                }
                /**
                 * 最小的磁盘占用率 达到 过期清理警戒线的磁盘空间占用率
                 */
                if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, "
                            + minPhysicRatio + ", storePathPhysic=" + minStorePath);
                    return true;
                }
            }

            {
                // 获取consumequeue文件存储路径
                String storePathLogics = DefaultMessageStore.this.getStorePathLogic();
                // 获取磁盘占用率
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                /**
                 * 磁盘占用率 达到 Broker拒绝消息写入的磁盘空间占用率，强制清除存储文件 并 设置磁盘满标记（停止写入）
                 */
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    /**
                     * 磁盘占用率 达到 强制清理的磁盘空间占用率，则强制清除存储文件
                     */
                    cleanImmediately = true;
                } else {
                    /**
                     * 设置磁盘为未满
                     */
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        // 磁盘满过后，再不满才会打印该日志
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }
                /**
                 * 磁盘占用率 达到 过期清理警戒线的磁盘空间占用率，则进行清理
                 */
                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }

        /**
         * DefaultMessageStore.CleanCommitLogService#calcStorePathPhysicRatio()
         * 计算CommitLog存储路径的最小磁盘使用率
         * @return
         */
        public double calcStorePathPhysicRatio() {
            // 达到强制清理的磁盘空间占用率的磁盘路径集合
            Set<String> fullStorePath = new HashSet<>();
            // 获取commitLog文件存储路径
            String storePath = getStorePathPhysic();
            // 拆分多个commitLog文件存储路径
            String[] paths = storePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            double minPhysicRatio = 100;
            for (String path : paths) {
                // 获取某个commitLog文件存储路径的磁盘使用率
                double physicRatio = UtilAll.isPathExists(path) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
                // 设置最小的磁盘使用率
                minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
                // 最小磁盘使用率 达到 强制清理的磁盘空间占用率（默认85%），则加入 达到强制清理的磁盘空间占用率的磁盘路径集合
                if (physicRatio > diskSpaceCleanForciblyRatio) {
                    fullStorePath.add(path);
                }
            }
            // 修改达到强制清理的磁盘空间占用率的磁盘路径集合
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            return minPhysicRatio;

        }

        /**
         * DefaultMessageStore.CleanCommitLogService#isSpaceFull()
         * 空间是否满了
         * @return
         */
        public boolean isSpaceFull() {
            // 计算CommitLog存储路径的最小磁盘使用率
            double physicRatio = calcStorePathPhysicRatio();
            // 获取过期清理警戒线的磁盘空间占用率
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            // CommitLog存储路径的最小磁盘使用率 > 过期清理警戒线的磁盘空间占用率，则打印日志
            if (physicRatio > ratio) {
                DefaultMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            /**
             * 最小的磁盘占用率 达到 Broker拒绝消息写入的磁盘空间占用率（默认90%），设置磁盘满标记（停止写入）
             */
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                //  设置磁盘满标记（停止写入）
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                /**
                 * 设置磁盘为未满标记
                 */
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    /**
     * 清除过期ConsumeQueue文件的服务： 处理 ConsumeQueue、IndexFile文件的过期删除
     */
    class CleanConsumeQueueService {
        // 上次删除时，CommitLog 的投递 Offset（最小的偏移量）
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                // 删除过期文件
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            // 1、删除物理文件（ConsumeQueue、IndexFile）的时间间隔。（两个文件的删除间隔）
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();
            // 2、查出当前 CommitLog 的投递 Offset（最小的偏移量）
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            // 3、当前 CommitLog 的投递 Offset（最小的偏移量） 大于 上次处理Offset（最小的偏移量），
            //    表明在这段时间内 CommitLog 有被删除掉，则ConsumeQueue、IndexFile需进行删除处理
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;
                // 3.1、删除逻辑队列文件
                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;
                // 3.2、遍历每个 ConsumeQueue，删除小于该 Offset 的 文件
                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        // 3.2.1、删除 小于 当前 CommitLog 的投递 Offset（最小的偏移量）的ConsumeQueue
                        int deleteCount = logic.deleteExpiredFile(minOffset);
                        // 3.2.2、清理文件后，需睡眠一段时间（100ms）
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }
                // 3.3 清理 IndexFile
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }
        // 获取类简称
        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * ConsumeQueue文件的刷盘服务
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 消息重放服务
     * 根据CommitLog文件 更新 index文件索引和ConsumeQueue文件偏移量的服务
     */
    class ReputMessageService extends ServiceThread {

        /**
         * 重放偏起始移量
         */
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }
        // 已存储在commitlog提交日志中但尚未分派到consume queue消费队列的字节数
        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * CommitLog是否需要执行重放
         * @return
         */
        private boolean isCommitLogAvailable() {
            // 重放偏移量是否小于commitlog的最大物理偏移量
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 执行重放
         */
        private void doReput() {
            // 如果重放偏移量（reputFromOffset）小于commitlog的最小物理偏移量，则设置为commitlog的最小物理偏移量
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                        this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            /**
             * 如果重放偏移量小于commitlog的最大物理偏移量，则循环重放
             */
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
                // 如果消息允许重复复制（默认为 false） 且 reputFromOffset大于等于已确定的偏移量confirmOffset，则结束循环
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }
                /**
                 * 根据reputFromOffset的物理偏移量找到mappedFileQueue中对应的CommitLog文件的MappedFile
                 * 然后从该MappedFile中截取一段自reputFromOffset偏移量开始的ByteBuffer，这段内存存储着将要重放的消息
                 */
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        // 将截取的起始物理偏移量设置为重放偏起始移量
                        this.reputFromOffset = result.getStartOffset();
                        /**
                         * 开始读取这段ByteBuffer中的消息，依次进行重放
                         */
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 检查消息的属性 并 构建一个DispatchRequest对象返回
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            // 消息大小，如果是基于Dledger技术的高可用DLedgerCommitLog，则取bufferSize
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            if (dispatchRequest.isSuccess()) {
                                // 如果大小大于0，表示有消息
                                if (size > 0) {
                                    /**
                                     * 分发请求
                                     * 1. CommitLogDispatcherBuildConsumeQueue：根据DispatchRequest写ConsumeQueue文件，构建ConsumeQueue索引。
                                     * 2. CommitLogDispatcherBuildIndex：根据DispatchRequest写IndexFile文件，构建IndexFile索引。
                                     * 3. CommitLogDispatcherCalcBitMap：根据DispatchRequest构建布隆过滤器，加速SQL92过滤效率，避免每次都解析sql。
                                     */
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);
                                    // 如果broker角色不是SLAVE 且 支持长轮询 且 消息送达的监听器不为null
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {
                                        /**
                                         * 通过该监听器的arriving方法触发调用pullRequestHoldService的pullRequestHoldService方法（即唤醒挂起的拉取消息请求，表示有新的消息落盘可进行拉取）
                                         * 涉及到RocketMQ的consumer消费push模式的实现
                                         */
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                        notifyMessageArrive4MultiQueue(dispatchRequest);
                                    }
                                    // 设置重放偏起始移量加上当前消息大小
                                    this.reputFromOffset += size;
                                    // 设置读取的大小加上当前消息大小
                                    readSize += size;
                                    // 如果是SLAVE角色，则存储数据的统计信息更新
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .add(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    /**
                                     * 如果等于0，表示读取到MappedFile文件尾
                                     * 获取下一个文件的起始索引
                                     */
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    // 设置readSize为0，将会结束循环
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                            DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    // 如果重做完毕，则跳出循环
                    doNext = false;
                }
            }
        }

        private void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
            Map<String, String> prop = dispatchRequest.getPropertiesMap();
            if (prop == null) {
                return;
            }
            String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
            String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
            if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
                return;
            }
            String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            if (queues.length != queueOffsets.length) {
                return;
            }
            for (int i = 0; i < queues.length; i++) {
                String queueName = queues[i];
                long queueOffset = Long.parseLong(queueOffsets[i]);
                int queueId = dispatchRequest.getQueueId();
                if (DefaultMessageStore.this.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                    queueId = 0;
                }
                DefaultMessageStore.this.messageArrivingListener.arriving(
                        queueName, queueId, queueOffset + 1, dispatchRequest.getTagsCode(),
                        dispatchRequest.getStoreTimestamp(), dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
            }
        }


        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");
            /**
             * 运行时逻辑
             * 如果服务未停止，则在死循环中执行重放的操作
             */
            while (!this.isStopped()) {
                try {
                    // 睡眠1ms
                    Thread.sleep(1);
                    // 执行重放
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
