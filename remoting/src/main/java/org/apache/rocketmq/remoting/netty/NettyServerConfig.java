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

/**
 * NettyServer的配置类
 *  NameServer、Broker作为服务端时 使用的各种属性
 */
public class NettyServerConfig implements Cloneable {
    /**
     * 监听端口
     * NameServer监听端口：9876
     * Broker监听端口（即客户端与Broker通信，remotingServer）：10911
     * 高可用通信监听端口（haListenPort）：Broker监听端口（普通服务的端口）+1（默认值：10912）
     * 快速Netty远程服务（fastRemotingServer）的配置监听的端口号：Broker监听端口（普通服务的端口）-2（即10909）
     */
    private int listenPort = 8888;
    /**
     * Netty远程通信执行器线程池（业务线程池） 的线程数
     * 默认值：8个
     */
    private int serverWorkerThreads = 8;
    /**
     * 服务器回调执行线程数，默认值为4
     * Netty public任务线程池线程个数
     * 根据业务类型会创建不同的线程池（比如：处理消息发送、消息消费、心跳检测等）。
     * 如果该业务类型（RequestCode）未注册线程池，则由public线程池执行。
     */
    private int serverCallbackExecutorThreads = 0;
    /**
     * server的work线程组数（IO 线程池线程个数）
     * 默认值：3个线程
     * NameServer、Broker端解析请求、返回相应的线程个数
     * 这类线程主要用于处理网络请求、解析请求包，再转发到各个业务线程池完成具体的业务操作，再将结果再返回调用方。
     */
    private int serverSelectorThreads = 3;
    /**
     * oneway 消息请求井发度（ Broker 端参数）
     * 默认值：256
     */
    private int serverOnewaySemaphoreValue = 256;
    /**
     * 异步消息发送最大并发度（ Broker 端参数）
     *  默认值：64
     */
    private int serverAsyncSemaphoreValue = 64;

    /**
     * 网络连接最大空闲时间
     * 测试端一定时间内未接受到被测试端消息和一定时间内向被测试端发送消息的超时时间为120秒（最大心跳时间、 最大空闲时间）
     * 默认值：120s 。
     * 如果连接空闲时间超过该参数设置的值，连接将被关闭。
     */
    private int serverChannelMaxIdleTimeSeconds = 120;
    /**
     * 网络socket发送缓存区大小，
     * 默认值：64k（65535）
     */
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    /**
     * 网络 socket接收缓存区大小
     * 默认值：64k（65535）
     */
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;
    private int serverSocketBacklog = NettySystemConfig.socketBacklog;
    /**
     * ByteBuffer 是否开启缓存
     * 默认开启
     * 建议开启。
     */
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * 是否启用Epoll IO 模型
     * Linux 环境建议开启
     * 默认不启用
     * make install
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    }
}
