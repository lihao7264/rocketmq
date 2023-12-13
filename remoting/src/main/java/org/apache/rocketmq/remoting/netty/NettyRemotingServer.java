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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 一个基于Netty的用于NameServer与Broker、Consumer、Producer进行网络通信的服务端
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    // Netty服务端启动类，引导启动服务端
    private final ServerBootstrap serverBootstrap;
    // worker EventLoopGroup（worker线程组用于处理读写事件）
    private final EventLoopGroup eventLoopGroupSelector;
    // Boss EventLoopGroup（boss线程组用于处理连接事件）
    private final EventLoopGroup eventLoopGroupBoss;
    // Netty服务的配置
    private final NettyServerConfig nettyServerConfig;

    /**
     * 默认执行器：publicExecutor
     * 默认线程数：4个线程
     * 线程名：以NettyServerPublicExecutor_为前缀
     */
    private final ExecutorService publicExecutor;
    /**
     * Netty事件执行器
     * NameServer中的事件执行器：BrokerHousekeepingService
     */
    private final ChannelEventListener channelEventListener;

    private final Timer timer = new Timer("ServerHouseKeepingService", true);
    /**
     * 默认事件处理器组，线程数默认8个线程，线程名以NettyServerCodecThread_为前缀。
     * 主要用于执行在真正执行业务逻辑之前需进行的SSL验证、编解码、空闲检查、网络连接管理等操作
     * 其工作时间位于IO线程组之后，process线程组之前
     */
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    /**
     * 端口号
     *  NameServer的端口：9876
     *  Broker的端口：10911
     */
    private int port = 0;

    /**
     * 用于处理TSL协议握手的Handler
     */
    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handlers
    /**
     * 用于处理TSL协议握手的Handler
     */
    private HandshakeHandler handshakeHandler;
    /**
     * RocketMQ自定义的编码器，处理报文的编码操作
     * 负责网络传输数据和 RemotingCommand 之间的编码
     */
    private NettyEncoder encoder;
    /**
     * 处理连接事件的handler：负责连接的激活、断开、超时、异常等事件
     */
    private NettyConnectManageHandler connectionManageHandler;
    /**
     *  处理读写事件的handler：真正处理业务请求的
     */
    private NettyServerHandler serverHandler;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
                               final ChannelEventListener channelEventListener) {
        // 设置服务器单向、异步发送信号量
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        // 创建Netty服务端启动类，引导启动服务端
        this.serverBootstrap = new ServerBootstrap();
        // Netty服务端配置类
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;
        // 服务器回调执行线程数量，默认值为4
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        /**
         * 创建一个公共线程池（线程数），负责处理某些请求业务
         *  举例：发送异步消息回调，线程名以NettyServerPublicExecutor_为前缀
         */
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
        /**
         * 是否使用epoll模型 并 初始化Boss EventLoopGroup（线程名以NettyEPOLLBoss_为前缀）和Worker EventLoopGroup（线程名以NettyServerEPOLLSelector_为前缀）这两个事件循环组
         * 如果是linux内核 且 指定开启epoll 且 系统支持epoll，才会使用EpollEventLoopGroup，否则使用NioEventLoopGroup
         * https://www.yuque.com/lijiaxiaodi/di99ys/hogwiikoskuamlq7#Eermz
         */
        if (useEpoll()) {
            /**
             *
             *  采用epoll 生成boss组，线程数为1
             **/
            this.eventLoopGroupBoss = new EpollEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyEPOLLBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            // 创建worker组 epoll的
            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            /**
             * 未采用epoll
             * Boss EventLoopGroup 默认1个线程，线程名以NettyNIOBoss_为前缀
             */
            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });
            /**
             * Worker EventLoopGroup 默认3个线程，线程名以NettyServerNIOSelector_为前缀
             */
            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }
        // 加载ssl信息
        loadSslContext();
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform()
                && nettyServerConfig.isUseEpollNativeSelector()
                && Epoll.isAvailable();
    }

    /**
     * 启动方法
     */
    @Override
    public void start() {
        /**
         * 1.创建默认事件处理器组，线程数默认8个线程，线程名以NettyServerCodecThread_为前缀。
         *   主要用于执行在真正执行业务逻辑之前需进行的SSL验证、编解码、空闲检查、网络连接管理等操作（即用于执行channelHandler逻辑）
         *   其工作时间位于IO线程组之后，process线程组之前
         */
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactory() {

                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }
                });
        /**
         * 2 准备一些共享handler（即Netty的ChannelHandler对象）
         *    包括handshakeHandler、encoder、connectionManageHandler、serverHandler
         */
        prepareSharableHandlers();
        /**
         * 3 配置NettyServer的启动参数
         *   包括handshakeHandler、encoder、connectionManageHandler、serverHandler
         */
        ServerBootstrap childHandler =
                /**
                 * 配置bossGroup为此前创建的eventLoopGroupBoss，默认1个线程，用于处理连接事件（accept事件）
                 * 配置workerGroup为此前创建的eventLoopGroupSelector，默认3个线程，用于处理IO事件（read和write事件）
                 * */
                this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                        // IO模型：根据是否是Linux内核，创建不同的socketChannel用于accept
                        .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                        /**
                         *  设置通道的选项参数， 对于服务端而言是ServerSocketChannel， 客户端而言是SocketChannel
                         *  option主要是针对boss线程组，child主要是针对worker线程组
                         */
                        /**
                         * 默认值1024
                         * 对应的是tcp/ip协议（具体实现为操作系统层面的协议栈代码）listen函数中的backlog参数（用于初始化服务端可连接队列）
                         * 服务端处理客户端的连接请求是顺序处理的，当服务器无过多线程处理连接请求时，需将客户端的连接请求放入到队列中
                         * SO_BACKLOG指定该队列的大小
                         * 内核要维护 两个队列：
                         *    未连接队列(syns queue)：保存客户端已发来Syn连接请求（表示至少一个Syn已到达），但还未完成三次握手
                         *    已连接队列(accept queue)：保存已完成三次握手，但还未调用 socket库的accept方法
                         * SO_BACKLOG的作用：当 syncQueue + acceptQueue > SO_BACKLOG时，新连接会被TCP内核拒绝掉
                         */
                        .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getServerSocketBacklog())
                        /**
                         * SO_REUSEADDR ：地址复用相关
                         * 对应于套接字选项中的SO_REUSEADDR：表示允许重复使用本地地址和端口
                         * 一个端口释放后会等待两分钟后才能再被使用，SO_REUSEADDR是让端口释放后立即可以再次被使用
                         */
                        .option(ChannelOption.SO_REUSEADDR, true)
                        /**
                         * 保活机制/心跳机制
                         * 对应于套接字选项中的SO_KEEPALIVE：用于设置TCP连接，当设置该选项后，连接会测试链接的状态
                         * 问题：如果开启SO_KEEPALIVE后，当客户端向服务端发送消息后，服务端未向客户端回复，则客户端可能不知道 服务器是否已挂掉。
                         * 解决办法：客户端当超过一定时间后自动给服务器发送一个空报文，等待服务端返回
                         *
                         * 关掉tcp保护机制的四个原因：
                         *  原因一：超时的默认时间为2小时，意味只有当服务端未向客户端响应数据，客户端需2小时后才能发现服务端的问题，参数是可配置的
                         *  原因二：保活机制是属于传输层的，当发现连接挂掉后不能执行应用层的相应逻辑
                         *  原因三：Namesrv实现了应用层的心跳机制，用于处理Broker下线问题
                         *  原因四：不能判断连接是否可用，只能判断连接是否存活，TCP连接中的另外一方突然断电关闭连接，则对端是无法知晓的。
                         *
                         */
                        .option(ChannelOption.SO_KEEPALIVE, false)
                        //  childOption：用于指定worker组线程（即客户端已和服务端完成了三次握手，进入read/write步骤）
                        /**
                         * 对应于套接字选项中的TCP_NODELAY：使用与Nagle算法有关
                         *   控制是否开启Nagle算法，提高较慢的广域网传输效率：减少需传输的数据次数，优化网络 既然相同的数据要减少传输次数，
                         *   则必要导致通信过程中数据包的增多
                         */
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        // 指定accept监听端口：9876
                        .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                        /**
                         *  设置用于为 Channel 的请求提供服务的 ChannelHandler
                         */
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) throws Exception {
                                /**
                                 * ChannelPipeline一个ChannelHandler的链表
                                 * Netty处理请求基于责任链（其中的ChannelHandler就用于处理请求）
                                 *  handshakeHandler、encoder、connectionManageHandler、serverHandler都标注了Sharable注解，表示公用Handler，无需为每个socket连接创建相应的handler
                                 * inboundHandler: handshakeHandler -> decoder -> idleState -> connectManager -> serverHandler
                                 * outboundHandler : encoder -> idleState -> connectManager -> serverHandler
                                 */
                                ch.pipeline()
                                        /**
                                         * handshakeHandler：处理TSL协议握手的Handler
                                         *  当客户端配置了useTls = true，为服务端动态创建SSLHandler 并 动态删除自己
                                         */
                                        .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                                        // 为defaultEventExecutorGroup，添加handler
                                        .addLast(defaultEventExecutorGroup,
                                                // RocketMQ自定义的请求解码器
                                                encoder,
                                                // RocketMQ自定义的请求编码器
                                                new NettyDecoder(),
                                                /**
                                                 * 心跳机制
                                                 * Netty自带的心跳管理器，主要用于检测远端是否存活（配置时间：120秒）
                                                 * 即测试端一定时间内未接受到被测试端消息和一定时间内向被测试端发送消息的超时时间为120秒
                                                 * （当 120s内 不存在 读｜写时，IdleStateHandler通过pipeline传递userEventTriggered()方法 且 内容类型为IdleStateEvent）
                                                 */
                                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                                /**
                                                 * connectionManageHandler（连接管理器处理器）：负责连接激活、断开、超时、异常等事件
                                                 *     即用于接受心跳事件，然后进行处理
                                                 *  用于当一定时间内发现对端连接未进行读｜写数据进行关闭通道
                                                 */
                                                connectionManageHandler,
                                                /**
                                                 * （核心）服务请求处理器：处理所有RemotingCommand消息（即请求和响应的业务处理） 并 返回相应的处理结果。
                                                 *      请求被封装为RemotingCommand对象，然后被serverHandler内部的逻辑处理
                                                 * 举例：broker注册、producer/consumer获取Broker、Topic信息等请求都是该处理器处理
                                                 *      serverHandler最终会将请求根据不同的消息类型code分发到不同的process线程池处理
                                                 */
                                                serverHandler
                                        );
                            }
                        });
        // 对应于套接字选项中的SO_SNDBUF（发送缓冲区），默认是65535
        if (nettyServerConfig.getServerSocketSndBufSize() > 0) {
            log.info("server set SO_SNDBUF to {}", nettyServerConfig.getServerSocketSndBufSize());
            childHandler.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize());
        }
        // 对应于套接字选项中的SO_SNDBUF（接收缓冲区），默认是65535
        if (nettyServerConfig.getServerSocketRcvBufSize() > 0) {
            log.info("server set SO_RCVBUF to {}", nettyServerConfig.getServerSocketRcvBufSize());
            childHandler.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize());
        }
        // 用于设置写缓冲区的低水位线和高水位线
        if (nettyServerConfig.getWriteBufferLowWaterMark() > 0 && nettyServerConfig.getWriteBufferHighWaterMark() > 0) {
            log.info("server set netty WRITE_BUFFER_WATER_MARK to {},{}",
                    nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark());
            childHandler.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()));
        }
        // 开启Netty内存池管理：分配缓冲区
        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            /**
             * 启动Netty服务：绑定端口，sync()同步等待
             */
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            // 设置并保存端口号，默认9876
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        /**
         * 如果channelEventListener不为null，则启动Netty事件执行器
         *   这里的listener就是之前初始化的BrokerHousekeepingService
         */
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
        /**
         * 启动定时任务，初始启动3秒后执行，此后每隔1秒执行一次（即执行周期1s）
         * 扫描responseTable，将超时的ResponseFuture直接移除 并 执行这些超时ResponseFuture的回调
         */
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    // 定期调用以扫描过期的请求
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    @Override
    public void shutdown() {
        try {
            if (this.timer != null) {
                this.timer.cancel();
            }

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupSelector.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    /**
     * 注册Netty请求处理器
     * @param requestCode 请求编码
     * @param processor 请求处理器
     * @param executor 请求执行器
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            // 默认执行器是publicExecutor，线程数默认4个线程，线程名以NettyServerPublicExecutor_为前缀
            executorThis = this.publicExecutor;
        }
        // 构建Pair对象
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        // 存入NettyRemotingServer的processorTable属性中
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        // defaultRequestProcessor属性
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }


    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    /**
     * 准备一些共享handler
     * 包括handshakeHandler、encoder、connectionManageHandler、serverHandler
     */
    private void prepareSharableHandlers() {
        handshakeHandler = new HandshakeHandler(TlsSystemConfig.tlsMode);
        encoder = new NettyEncoder();
        connectionManageHandler = new NettyConnectManageHandler();
        serverHandler = new NettyServerHandler();
    }

    @ChannelHandler.Sharable
    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                    .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                    .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish an SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    /**
     * 处理服务端请求
     */
    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * channelRead0() 与 channelRead() 方法的区别：channelRead0只关注数据类型为RemotingCommand的数据
         * @param ctx
         * @param msg
         * @throws Exception
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            // Netty服务端业务请求处理器的入口
            processMessageReceived(ctx, msg);
        }
    }

    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 当RocketMQ配置时间 默认120秒 期间此channel未进行 读｜写操作时，则通道内会传播此方法
         * @param ctx
         * @param evt
         * @throws Exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    // 关闭通道
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
