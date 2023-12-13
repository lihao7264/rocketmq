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
package org.apache.rocketmq.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RequestType;

/**
 * 客户端通用配置
 * Client Common configuration
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    /**
     * NameServer的地址列表
     * 从-D系统参数（rocketmq.namesrv.addr）或环境变量（NAMESRV_ADDR）
     */
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    /**
     * 客户端IP
     */
    private String clientIP = RemotingUtil.getLocalAddress();
    /**
     * 客户端实例名称
     * 从-D系统参数（rocketmq.client.name）获取，否则是DEFAULT
     */
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    /**
     * 客户端通信层接收到网络请求时，处理器的核数
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    /**
     * 客户端命名空间
     * NameSpace地址
     */
    protected String namespace;
    private boolean namespaceInitialized = false;
    /**
     * 设置访问通道
     * LOCAL、CLOUD
     */
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * 轮询从NameServer获取路由信息的时间间隔
     * 默认值：30s
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * 定期发送注册心跳到broker的间隔
     * 默认值：30s
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * 作用于Consumer，持久化消费进度的间隔
     * 默认值：5s
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    /**
     * consumer 状态错误时，采用定时任务定时执行拉取请求的时间间隔（拉取消息出现异常的延迟时间设置）
     * 默认值：1s
     */
    private long pullTimeDelayMillsWhenException = 1000;
    /**
     * 单位模式
     */
    private boolean unitMode = false;
    /**
     * 单位名称
     */
    private String unitName;
    /**
     * 是否启用vip Netty通道以发送消息
     * 默认false
     * 从-D com.rocketmq.sendMessageWithVIPChannel参数的值，若无则是true
     */
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

    /**
     * 是否使用安全传输
     * 从-D系统参数tls.enable获取，否则是false
     */
    private boolean useTLS = TlsSystemConfig.tlsEnable;

    /**
     * mq客户端api超时设置（请求超时时间）
     * 默认值：3s
     */
    private int mqClientApiTimeout = 3 * 1000;

    /**
     * 客户端实现语言
     */
    private LanguageCode language = LanguageCode.JAVA;

    /**
     * Enable stream request type will inject a RPCHook to add corresponding request type to remoting layer.
     * And it will also generate a different client id to prevent unexpected reuses of MQClientInstance.
     */
    protected boolean enableStreamRequestType = false;

    /**
     * 构建clientId
     * 格式：clientIP@instanceName@unitName
     */
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
        }
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.namespace = cc.namespace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
        this.enableStreamRequestType = cc.enableStreamRequestType;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.namespace = namespace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        cc.enableStreamRequestType = enableStreamRequestType;
        return cc;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
        }
        return namesrvAddr;
    }

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     *
     * @param namesrvAddr name server address
     */
    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public String getNamespace() {
        if (namespaceInitialized) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                namespace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespaceInitialized = true;
    }

    public AccessChannel getAccessChannel() {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public boolean isEnableStreamRequestType() {
        return enableStreamRequestType;
    }

    public void setEnableStreamRequestType(boolean enableStreamRequestType) {
        this.enableStreamRequestType = enableStreamRequestType;
    }

    @Override
    public String toString() {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName=" + instanceName
                + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInterval=" + pollNameServerInterval
                + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval + ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval
                + ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException + ", unitMode=" + unitMode + ", unitName=" + unitName + ", vipChannelEnabled="
                + vipChannelEnabled + ", useTLS=" + useTLS + ", language=" + language.name() + ", namespace=" + namespace + ", mqClientApiTimeout=" + mqClientApiTimeout
                + ", enableStreamRequestType=" + enableStreamRequestType + "]";
    }
}
