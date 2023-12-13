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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

/**
 * NettyRemotingAbstract抽象类
 * NettyRemotingServer、NettyRemotingClient的父类
 */
public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);


    /**
     * oneway 消息请求井发度
     *  客户端单向发送的Semaphore信号量默认为65535，可通过配置"com.rocketmq.remoting.clientOnewaySemaphoreValue"系统变量更改
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 异步消息发送最大并发度（ Broker 端参数）
     * 客户端异步发送的Semaphore信号量默认为65535，可通过配置"com.rocketmq.remoting.clientAsyncSemaphoreValue"系统变量更改
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * 缓存所有正在进行处理的请求
     * <请求id,响应ResponseFuture>：响应结果缓存
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * 处理器表：<请求code,{请求处理器,执行线程池}>
     *         包含每个请求代码的所有处理器。
     * 对于每个传入的请求，可在该映射中查找响应的处理器来处理请求。
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * 默认请求处理器
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();


    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     *  处理RemotingCommand命令消息，传入的远程处理命令可能：
     *    1、来自远程对等组件的查询请求
     *    2、对该参与者之前发出的请求的响应
     * @param ctx     Netty层面 channelHandler上下文
     * @param msg    RocketMQ通信层交互对象
     * @throws Exception
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                /**
                 * 根据 命令类型 判断是 请求命令还是响应命令
                 * 具体判断逻辑：请求头中的flag属性
                 *  flag字段低1位为0：表示请求命令
                 *  flag字段低1位为1：表示响应命令
                 *
                 */
                // 处理来源服务端的请求request
                case REQUEST_COMMAND:
                    // 客户端发送消息后，请求会在服务端被processRequestCommand方法处理
                    processRequestCommand(ctx, cmd);
                    break;
                // 处理来源服务端的响应response
                case RESPONSE_COMMAND:
                    // 客户端发送消息之后服务端的响应会被processResponseCommand方法处理
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }


    /**
     * 处理远程对等方发出的传入请求命令
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据 RemotingCommand 的业务请求码code去processorTable 本地缓存变量中找到对应的 processor及对应的处理线程池
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 如果该Code未注册的RequestProcessor，则采用DefaultRequestProcessor作为默认请求处理器，使用remotingExecutor作为默认请求执行器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        // 获取该请求的唯一id
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            /**
             * 1.创建一个用于执行请求处理的线程任务
             */
            Runnable run = new Runnable() {
                /**
                 * 执行此方法时， 说明已被包装为RequestTask对象 且 提交到线程池
                 */
                @Override
                public void run() {
                    try {
                        // 获取远程地址
                        String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                        // 执行前置钩子方法（doBeforeRequest方法）
                        doBeforeRpcHooks(remoteAddr, cmd);
                        /**
                         * 创建响应回调函数
                         */
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                // 执行后置钩子方法
                                doAfterRpcHooks(remoteAddr, cmd, response);
                                // 如果不是单向消息
                                if (!cmd.isOnewayRPC()) {
                                    // 如果响应不为null
                                    if (response != null) {
                                        // 设置响应id为请求id，标记响应状态
                                        response.setOpaque(opaque);
                                        // 标记为响应类型数据（即flag字段的低0位的值是1）
                                        response.markResponseType();
                                        response.setSerializeTypeCurrentRPC(cmd.getSerializeTypeCurrentRPC());
                                        try {
                                            // 将响应写回给客户端
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                    }
                                }
                            }
                        };
                        /**
                         * 调用处理器处理请求
                         */
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            /**
                             *  如果处理器是异步请求处理器（AsyncNettyRequestProcessor），则调用异步处理的方法asyncProcessRequest
                             *  SendMessageProcessor、DefaultRequestProcessor是一个异步消息处理器
                             *
                             *  DefaultRequestProcessor.asyncProcessRequest()和DefaultRequestProcessor.processRequest()无区别
                             *  其它处理器可能有区别
                             */
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor)pair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            // 如果处理器不是异步请求处理器，则调用同步处理的方法processRequest获取响应，然后同步调用callback回调方法
                            NettyRequestProcessor processor = pair.getObject1();
                            // 处理同步请求
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            // 处理完成后调用回调方法
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());
                        // 如果不是单向请求，则返回系统异常的响应SYSTEM_ERROR
                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                    RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };
            /**
             * 2.如果该请求处理器拒绝该请求，则返回系统繁忙的响应SYSTEM_BUSY
             */
            if (pair.getObject1().rejectRequest()) {
                // 创建 响应对象 响应码类型为 系统忙碌SYSTEM_BUSY（2）
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[REJECTREQUEST]system busy, start flow control for a while");
                // 设置请求的唯一id
                response.setOpaque(opaque);
                // 写回对端
                ctx.writeAndFlush(response);
                return;
            }
            /**
             * 3.构建请求线程任务，再通过执行器线程池执行
             */
            try {
                // 构建线程任务（RequestTask对象）
                /**
                 * 参数一：runnable对象
                 * 参数二：通道关联的channel
                 * 参数三：cmd对象（通信对象）
                 */
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                /**
                 *  向线程池提交requestTask任务，通过对应的请求执行器执行，这里支持多线程并发的执行请求处理
                 *  pair中存在两个对象：
                 *   object1：处理器
                 *   object2：执行此处理器逻辑的线程池
                 */
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                            + ", too many requests and system thread pool busy, RejectedExecutionException "
                            + pair.getObject2().toString()
                            + " request code: " + cmd.getCode());
                }
                // 返回系统繁忙响应
                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                            "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            // 未找到任何请求处理器，返回不支持该请求code的响应
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * 客户端发送消息之后服务端的响应会被processResponseCommand方法处理
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // 获取请求id，通过id可以获取请求结果
        final int opaque = cmd.getOpaque();
        /**
         * 根据请求标识（请求id）找到之前放到responseTable的ResponseFuture
         *  如果未获取到说，明可能请求超时了
         */
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            // 设置结果
            responseFuture.setResponseCommand(cmd);
            // 从responseTable中移除该响应
            responseTable.remove(opaque);
            // 条件成立：说明是异步 执行异步回调逻辑
            if (responseFuture.getInvokeCallback() != null) {
                /**
                 * 如果存在回调函数，即异步请求
                 * 则调用回调函数的方法
                 */
                executeInvokeCallback(responseFuture);
            } else {
                /**
                 * 如果是同步请求，生产者线程此时正处于阻塞，则调用putResponse方法
                 * 该方法将响应数据设置到responseCommand，再调用countDownLatch.countDown（即倒计数减去1，唤醒等待的线程）
                 */
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * 在回调执行器中执行回调操作，如果回调执行器为null，则在当前线程中执行回调
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        // 获取回调执行器，如果未设置回调执行器callbackExecutor（默认没有），则使用publicExecutor
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 通过线程池异步的执行回调操作
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }
        // 在本线程中执行回调操作
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }



    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }


    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * 扫描responseTable，将超时的ResponseFuture直接移除 并 执行这些超时ResponseFuture的回调
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();
            // 超时，则移除
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }
        // 回调需移除的 ResponseFuture回调
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 执行同步调用
     * @param channel     channel
     * @param request     请求对象
     * @param timeoutMillis  更新后的超时时间
     * @return
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 获取请求id，通过id可获取请求结果（一个自增的整数值：requestId.getAndIncrement() 类似于请求唯一id）
        final int opaque = request.getOpaque();

        try {
            /**
             * 创建ResponseFuture对象
             * 参数一：channel
             * 参数二：请求对象
             * 参数三：超时时间
             * 参数四：由于是同步调用 因此无回调为null
             * 参数五：是否是单向请求，不是的话为null
             */
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 将请求id和responseFuture存入responseTable缓存中
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            // 发送请求，添加一个ChannelFutureListener，消息发送完毕会进行回调
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    // 如果消息发送成功，则设置responseFuture发送成功并返回
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }
                    // 如果发送消息失败，则从responseTable移除该缓存
                    responseTable.remove(opaque);
                    // 设置失败原因
                    responseFuture.setCause(f.cause());
                    /**
                     * 设置响应结果为null，唤醒阻塞的responseFuture
                     * 内部调用countDownLatch.countDown()方法
                     */
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });
            /**
             * responseFuture同步阻塞等待直到得到响应结果 或 到达超时时间
             * 内部调用了countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS)方法
             */
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            /**
             *  执行到这里的两种可能
             *  1.在指定超时时间内，对端返回了数据
             *  2.到达超时时间
             */
            // 如果响应结果为null
            if (null == responseCommand) {
                // 如果是发送成功，但无响应，则表示等待响应超时，则抛出超时异常
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    // 如果是发送失败，抛出发送失败异常
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            // 执行到这里，说明在指定时间内返回了数据，返回响应对象
            return responseCommand;
        } finally {
            // 从responseTable中移除请求缓存映射
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步调用实现
     * @param channel       channel
     * @param request       请求对象 网络交互对象
     * @param timeoutMillis  超时时间
     * @param invokeCallback  结果回调函数
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 起始时间
        long beginStartTime = System.currentTimeMillis();
        // 获取请求id，通过id可获取请求结果
        final int opaque = request.getOpaque();
        /**
         *  基于Semaphore信号量尝试获取异步发送的资源，通过信号量控制异步消息并发发送的消息数，从而保护系统内存占用。
         *  客户端异步发送的Semaphore信号量默认为65535，可通过配置"com.rocketmq.remoting.clientOnewaySemaphoreValue"系统变量更改
         */
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        // 如果获取到信号量资源
        if (acquired) {
            // 构建SemaphoreReleaseOnlyOnce对象，保证信号量本次只被释放一次，防止并发操作引起线程安全问题
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            // 如果超时，则不发送，抛出异常
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }
            // 创建一个Future的map成员ResponseFuture，设置超时时间、回调函数
            /**
             * 创建ResponseFuture对象
             * 参数一：channel
             * 参数二：请求对象
             * 参数三：超时时间
             * 参数四：回调函数
             * 参数五：后续用于释放获取到的信号量
             *
             * responseFuture中保存 回调函数，可利用ResponseFuture执行回调函数
             */
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            /**
             * 将请求id和responseFuture存入responseTable映射表中
             *  所有已发送未完成的请求都缓存在此表中，用于判断是否超时及实现线程阻塞等逻辑
             */
            this.responseTable.put(opaque, responseFuture);
            try {
                // 发送请求（将数据写入对端），添加一个ChannelFutureListener，消息发送完毕会进行回调
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 如果消息发送成功，则设置responseFuture发送成功并返回
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        /**
                         * 如果发送失败，则移除缓存、设置false 并 执行InvokeCallback#operationComplete回调
                         */
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                // 释放信号量
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            // 如果未获取到信号量资源，则直接抛出异常即可 且 不再发送
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis,
                                this.semaphoreAsync.getQueueLength(),
                                this.semaphoreAsync.availablePermits()
                        );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * 单向调用：单向发送
     * @param channel  通道
     * @param request    请求
     * @param timeoutMillis  超时时间
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        /**
         * 标记为单向发送
         * 将flag字段的低2位的值更改为1
         */
        request.markOnewayRPC();
        /**
         *  基于Semaphore信号量尝试获取单向发送的资源，通过信号量控制单向消息并发送的消息数，从而保护系统内存占用。
         *  客户端单向发送的Semaphore信号量默认为65535，可通过配置"com.rocketmq.remoting.clientOnewaySemaphoreValue"系统变量更改
         */
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        // 如果获取到了信号量资源
        if (acquired) {
            // 用于释放获取的信号量：构建SemaphoreReleaseOnlyOnce对象，保证信号量本次只被释放一次，防止并发操作引起线程安全问题
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                // 将请求发送出去即可
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 写入完成 回调监听器的此方法
                        // 释放信号量
                        once.release();
                        // 如果发送失败了，仅打印一行warn日志，然后就不管了
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                // 释放信号量
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            // 如果未获取到信号量资源，则直接抛出异常即可 并 不再发送
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                        timeoutMillis,
                        this.semaphoreOneway.getQueueLength(),
                        this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        /**
         * 接收netty事件的队列
         *  比如：AllIdle事件（心跳的）
         */
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            int currentSize = this.eventQueue.size();
            if (currentSize <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                // 心跳到期事件处理
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
