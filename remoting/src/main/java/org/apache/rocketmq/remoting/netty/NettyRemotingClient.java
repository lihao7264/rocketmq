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
package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Netty客户端实现
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * channelTables map的锁
     */
    private final Lock lockChannelTables = new ReentrantLock();
    /**
     * <目标地址,连接（Channel）>
     */
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    /**
     * nameserver地址集合
     */
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    /**
     * 上次被选择的NameServer地址
     */
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    /**
     * NameServer的索引
     * 初始化数据是随机生成的
     */
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock namesrvChannelLock = new ReentrantLock();

    private final ExecutorService publicExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig, final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        // 公共线程池的默认线程数：4
        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        // 创建公共线程池，默认内部线程数为4
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        // 生成worker组
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }

    /**
     * 创建Netty客户端，并未真正启动
     */
    @Override
    public void start() {
        /**
         *   创建默认事件处理器组（线程组），默认4个线程，线程名以NettyClientWorkerThread_为前缀。
         *    主要用于执行在真正执行业务逻辑之前需要进行的SSL验证、编解码、空闲检查、网络连接管理等操作（即执行配置的ChannelHandler逻辑）
         *    工作时间位于IO线程组之后，process线程组之前
         */
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(), new ThreadFactory() {

            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
            }
        });
        /**
         * 初始化Netty客户端
         * 通过客户端启动类配置worker组（eventLoopGroupWorker线程组）， 由于是客户端，因此默认线程数为1
         * 配置参数和NettyRemotingServer差不多
         * 唯一不同点：
         *    NettyRemotingServer配置的最后一个Handler为NettyServerHandler
         *    NettyRemotingClient配置的最后一个Handler为NettyClientHandler
         */
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                // 对应于套接字选项中的TCP_NODELAY，该参数的使用与Nagle算法有关
                .option(ChannelOption.TCP_NODELAY, true)
                // 对应于套接字选项中的SO_KEEPALIVE，该参数用于设置TCP连接，当设置该选项以后，连接会测试链接的状态
                .option(ChannelOption.SO_KEEPALIVE, false)
                // 用于设置连接超时时长，单位是毫秒，默认3000
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis()).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        if (nettyClientConfig.isUseTLS()) {
                            if (null != sslContext) {
                                pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                                log.info("Prepend SSL handler");
                            } else {
                                log.warn("Connections are insecure as SSLContext is null!");
                            }
                        }
                        // 为defaultEventExecutorGroup，添加handler
                        pipeline.addLast(defaultEventExecutorGroup,
                                // RocketMQ自定义的请求解码器
                                new NettyEncoder(),
                                // RocketMQ自定义的请求编码器
                                new NettyDecoder(),
                                /**
                                 * Netty自带的心跳管理器，主要用于检测远端是否存活
                                 * 即测试端一定时间内未接收到被测试端消息 和 一定时间内向被测试端发送消息的超时时间为120秒
                                 */
                                new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                // 连接管理器：负责连接激活、断开、超时、异常等事件
                                new NettyConnectManageHandler(),
                                /**
                                 * （重点）服务请求处理器，处理RemotingCommand消息（即请求和响应的业务处理，且返回相应的处理结果）。
                                 *  举例：broker注册、producer/consumer获取Broker、Topic信息等请求都是该处理器处理
                                 *   serverHandler最终会将请求根据不同的消息类型code分发到不同的process线程池处理
                                 */
                                new NettyClientHandler());
                    }
                });
        if (nettyClientConfig.getClientSocketSndBufSize() > 0) {
            log.info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
            handler.option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize());
        }
        if (nettyClientConfig.getClientSocketRcvBufSize() > 0) {
            log.info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
            handler.option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize());
        }
        if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0) {
            log.info("client set netty WRITE_BUFFER_WATER_MARK to {},{}", nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
            handler.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
        }
        /**
         * 启动定时任务，初始启动3秒后执行，此后每隔1秒执行一次
         * 扫描responseTable，将超时的ResponseFuture直接移除（移除过期的请求） 且 执行这些超时ResponseFuture的回调
         */
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
        /**
         * 启动netty事件监听器，处理各种事件
         */
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) return;

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                if (!addrs.contains(this.namesrvAddrChoosed.get())) {
                    this.namesrvAddrChoosed.set(null);
                }
            }
        }
    }

    /**
     * 同步调用
     *
     * @param addr          broker地址 或 namesrv地址
     * @param request       消息信息 网络传输对象
     * @param timeoutMillis 超时时间
     * @return
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        // 发送请求开始时间
        long beginStartTime = System.currentTimeMillis();
        // 根据addr建立连接，获取/创建channel
        // 以生产者为例：生产者向 namesrv 或 broker 发起socket连接，然后获取到连接成功的通道
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                // 执行rpc钩子的doBeforeRequest方法
                doBeforeRpcHooks(addr, request);
                // 检查超时，如果超时则抛出异常
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + addr + "] timeout");
                }
                // 执行同步远程调用，获得调用结果
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                // 执行rpc钩子的doAfterResponse方法
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 获取并建立连接
     *
     * @param addr 地址 有可能为空，有可能不为空
     *             以生产者举例：当需和NameServer交互进行获取路由信息时，addr为null
     *             当需和broker交互进行发送消息时，addr为具体的broker地址
     * @return 连接成功后返回的channel
     */
    private Channel getAndCreateChannel(final String addr) throws RemotingConnectException, InterruptedException {
        /**
         * 条件成立：如果addr为null，则获取并创建NameServer长连接
         *  随机选择NameServer连接
         */
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        /**
         * 执行到这里，说明是和broker进行交互，尝试从缓存中获取已存在的长连接（Channel）
         *    获取不到才会创建
         */
        ChannelWrapper cw = this.channelTables.get(addr);
        // 条件成立：说明从缓存中获取到Channel 直接返回
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        // 执行到这里，说明需进行connect操作 且 connect操作完成后将channel放入缓存（channelTables）
        return this.createChannel(addr);
    }

    /**
     * 获取并建立nameServer连接
     */
    private Channel getAndCreateNameserverChannel() throws RemotingConnectException, InterruptedException {
        /**
         * 如果此前已选择（上次被选择）了NameServer地址 且 通道存在 且 是激活状态，则直接获取该NameServer的通道
         *  即通道可被不同的consumer和producer复用
         */
        String addr = this.namesrvAddrChoosed.get();
        // 条件成立：说明上次存在被选择的NameServer地址
        if (addr != null) {
            // 根据地址到缓存中获取现有的channel
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                // 条件成立：说明存在现有的channel 直接返回
                return cw.getChannel();
            }
        }
        // 执行到这里，说明可能缓存中不存在channel 或 第一次进行连NameServer

        /**
         * 获取到指定的namesrv地址列表（即创建Producer或Consumer时指定的namesrv列表）
         * 可能存在多个，因为namesrv支持集群化部署
         */
        final List<String> addrList = this.namesrvAddrList.get();
        /**
         * 加锁
         * 双重检查机制
         */
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                // 再次获取上次被选择的nameServer地址：再次判断，防止并发
                addr = this.namesrvAddrChoosed.get();
                // 条件成立：说明上次存在被选择的NameServer地址
                if (addr != null) {
                    // 再次根据地址到缓存中获取现有的channel
                    ChannelWrapper cw = this.channelTables.get(addr);
                    // 条件成立：说明存在现有的channel 直接返回
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }
                /**
                 * 选择一个NameServer建立长连接
                 * 负载均衡的从namesrv列表中随机获取一个namesrv尝试进行连接创建 并 连接channel
                 *  且 将channel和选择的Namesrv地址都缓存起来，方便下次直接获取
                 */
                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        // 随机获取一个namesrv地址
                        // 获取下一个NameServer的索引，初始化数据是随机生成的
                        int index = this.namesrvIndex.incrementAndGet();
                        // 取绝对值
                        index = Math.abs(index);
                        // 对NameServer集合长度取余数
                        index = index % addrList.size();
                        // 获取最终确定的NameServer地址
                        String newAddr = addrList.get(index);
                        // 设置到被选择的namesrvAddrChoosed属性中
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        // 针对被选择的地址建立长连接（Channel）
                        Channel channelNew = this.createChannel(newAddr);
                        /**
                         *  如果建立连接成功，则返回。
                         *  如果建立连接失败，则则继续循环选择下一个NameServer地址创建
                         */
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    // 没有一个NameServer地址建立成功，则抛出异常
                    throw new RemotingConnectException(addrList.toString());
                }
            } finally {
                // 解锁
                this.namesrvChannelLock.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建连接
     * @param addr  目标地址
     * @return
     * @throws InterruptedException
     */
    private Channel createChannel(final String addr) throws InterruptedException {
        // 双重检查
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        // 加锁
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                // 是否需创建new连接（新连接）
                boolean createNewConnection;
                // 再次从缓存中获取channel
                cw = this.channelTables.get(addr);
                // 条件成立：说明缓存中获取到channel
                if (cw != null) {
                    // 条件成立：说明连接可用 返回连接
                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        // 执行到这里，说明连接不可用，则移除缓存 且 重新创建channel
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }
                // 条件成立：说明需创建new连接
                if (createNewConnection) {
                    /**
                     * 进行connect操作连接到对端
                     * 这里返回Future对象 connect操作是异步进行的，因此可能连接不一定成功，可能仍处于连接中
                     */
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    // 包装channelFuture，再放入channel映射表，方便获取获取
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            // 尝试等待3秒，等待连接操作完成
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture, channelFuture.cause());
                }
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(), channelFuture.toString());
            }
        }

        return null;
    }

    /**
     * 异步发送消息
     *
     * @param addr
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     */
    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 开始时间
        long beginStartTime = System.currentTimeMillis();
        // 获取或创建一个与broker的连接
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                // 执行rpc钩子的doBeforeRequest方法
                doBeforeRpcHooks(addr, request);
                // 如果超时，则直接抛出异常，不再执行
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call the addr[" + addr + "] timeout");
                }
                // 执行异步远程调用
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 单向消息发送的通用方法
     *
     * @param addr          服务器地址
     * @param request       请求命令对象
     * @param timeoutMillis 超时时间
     */
    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 获取/建立同服务器的通道（即连接）
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                // 执行rpcHook的前置方法doBeforeRequest
                doBeforeRpcHooks(addr, request);
                /**
                 * 调用另一个invokeOnewayImpl方法，发送单向消息
                 */
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    /**
     * 获取回调执行器，如果未设置回调执行器callbackExecutor（默认没有），则使用publicExecutor
     *
     * @return
     */
    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    /**
     * 处理来自服务端的RemotingCommand消息
     */
    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * 处理来自服务端的RemotingCommand消息
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
