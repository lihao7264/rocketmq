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
package org.apache.rocketmq.broker.processor;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 发送消息处理器
 * 一个异步消息处理器
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = null;
        try {
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request.toString(), e);
        }
        return response;
    }

    /**
     * 异步处理请求，默认走该方法
     * @param ctx
     * @param request
     * @param responseCallback
     * @throws Exception
     */
    @Override
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        /**
         * 1.调用asyncProcessRequest处理请求
         * 2.调用thenAcceptAsync异步的执行回调
         */
        asyncProcessRequest(ctx, request).thenAcceptAsync(responseCallback::callback, this.brokerController.getPutMessageFutureExecutor());
    }

    /**
     * 异步的处理请求
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx,
                                                                  RemotingCommand request) throws RemotingCommandException {
        final SendMessageContext mqtraceContext;
        /**
         * 根据不同的请求code选择不同的处理方式
         */
        switch (request.getCode()) {
            /**
             * 如果是消费者发送的消息回退请求，该请求用于实现消息重试
             * 如果消息消费失败，则消息将被通过回退请求发送回broker 并 延迟一段时间再消费
             */
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.asyncConsumerSendMsgBack(ctx, request);
            // 其它情况都属于生产者发送消息的请求，统一处理
            default:
                // 解析请求头
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    // 如果请求头为null，则返回一个null值结果
                    return CompletableFuture.completedFuture(null);
                }
                // 构建发送请求消息轨迹上下文
                mqtraceContext = buildMsgContext(ctx, requestHeader);
                // 执行发送消息前钩子方法
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
                if (requestHeader.isBatch()) {
                    // 处理批量发送消息逻辑
                    return this.asyncSendBatchMessage(ctx, request, mqtraceContext, requestHeader);
                } else {
                    /**
                     * 处理其它发送消息逻辑
                     * 举例：单条消息
                     */
                    return this.asyncSendMessage(ctx, request, mqtraceContext, requestHeader);
                }
        }
    }

    /**
     * 是否需拒绝处理该请求
     * @return
     */
    @Override
    public boolean rejectRequest() {
        /**
         * 两个条件只要有一个不满足，则拒绝处理该请求
         * 条件一：检查操作系统页缓存PageCache是否繁忙。
         * 条件二：检查临时存储池transientStorePool是否不足。
         *
         */
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
                this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx,
                                                                        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader)request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            ConsumeMessageContext context = buildConsumeMessageContext(namespace, requestHeader, request);
            this.executeConsumeMessageHookAfter(context);
        }
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return CompletableFuture.completedFuture(response);
        }
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return CompletableFuture.completedFuture(response);
        }

        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return CompletableFuture.completedFuture(response);
        }

        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        int queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % subscriptionGroupConfig.getRetryQueueNums();
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                newTopic,
                subscriptionGroupConfig.getRetryQueueNums(),
                PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return CompletableFuture.completedFuture(response);
        }

        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return CompletableFuture.completedFuture(response);
        }
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return CompletableFuture.completedFuture(response);
        }

        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        int delayLevel = requestHeader.getDelayLevel();

        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            Integer times = requestHeader.getMaxReconsumeTimes();
            if (times != null) {
                maxReconsumeTimes = times;
            }
        }

        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes
                || delayLevel < 0) {
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE | PermName.PERM_READ, 0);

            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return CompletableFuture.completedFuture(response);
            }
            msgExt.setDelayTimeLevel(0);
        } else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }
            msgExt.setDelayTimeLevel(delayLevel);
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        return putMessageResult.thenApply(r -> {
            if (r != null) {
                switch (r.getPutMessageStatus()) {
                    case PUT_OK:
                        String backTopic = msgExt.getTopic();
                        String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        if (correctTopic != null) {
                            backTopic = correctTopic;
                        }
                        if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msgInner.getTopic())) {
                            this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                            this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), r.getAppendMessageResult().getWroteBytes());
                            this.brokerController.getBrokerStatsManager().incQueuePutNums(msgInner.getTopic(), msgInner.getQueueId());
                            this.brokerController.getBrokerStatsManager().incQueuePutSize(msgInner.getTopic(), msgInner.getQueueId(), r.getAppendMessageResult().getWroteBytes());
                        }
                        this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);
                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                        return response;
                    default:
                        break;
                }
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(r.getPutMessageStatus().name());
                return response;
            }
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
            return response;
        });
    }

    /**
     * 处理单条消息
     * @param ctx
     * @param request
     * @param mqtraceContext
     * @param requestHeader
     * @return
     */
    private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                SendMessageContext mqtraceContext,
                                                                SendMessageRequestHeader requestHeader) {
        /**
         * 1.创建响应的命令对象，包括自动创建topic的逻辑
         */
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        // 获取响应头
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }
        // 获取消息体
        final byte[] body = request.getBody();
        // 获取队列id
        int queueIdInt = requestHeader.getQueueId();
        // 从broker的topicConfigTable缓存中根据topicName获取TopicConfig
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        // 如果队列id小于0，则随机选择一个写队列索引作为id
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }
        // 构建消息对象，保存着要存入commitLog的数据
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        // 设置topic
        msgInner.setTopic(requestHeader.getTopic());
        // 设置队列id
        msgInner.setQueueId(queueIdInt);
        /**
         * 2.处理重试和死信队列消息，将会对死信消息替换为死信topic
         */
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return CompletableFuture.completedFuture(response);
        }
        /**
         * 设置一系列属性
         */
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        Map<String, String> origProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        // 设置到properties属性中
        MessageAccessor.setProperties(msgInner, origProps);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        // WAIT属性: 表示消息发送时是否等消息存储完成后再返回
        if (origProps.containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            // 无需存储"WAIT=true"属性，从propertiesString中移除它，为每个消息节省9个字节。
            String waitStoreMsgOKValue = origProps.remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            // 将没有WAIT属性的origProps存入msgInner的propertiesString属性
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            // 将WAIT属性重新存入origProps集合中，因为msgInner.isWaitStoreMsgOK()稍后将被调用
            origProps.put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            // 将没有WAIT属性的origProps存入msgInner的propertiesString属性
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
        /**
         * 处理事务消息逻辑
         */
        // TRAN_MSG属性值：true（表示为事务消息）
        CompletableFuture<PutMessageResult> putMessageResult = null;
        // 处理事务消息
        String transFlag = origProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(transFlag)) {
            /**
             * 判断是否需拒绝事务消息
             * 如果需拒绝，则返回NO_PERMISSION异常
             */
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return CompletableFuture.completedFuture(response);
            }
            // 调用asyncPrepareMessage方法以异步方式处理、存储事务准备消息，底层仍是asyncPutMessage方法
            putMessageResult = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
        } else {
            /**
             * 不是事务消息，则调用asyncPutMessage方法处理，存储消息
             * 以异步方式将消息存储到存储器中，处理器可处理下一个请求而不是等待结果，当结果完成时，以异步方式通知客户端
             */
            putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        }
        // 处理消息存放的结果
        return handlePutMessageResultFuture(putMessageResult, response, request, msgInner, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    /**
     * 处理消息存放结果
     * @param putMessageResult    存放结果
     * @param response            响应对象
     * @param request             请求对象
     * @param msgInner            内部消息对象
     * @param responseHeader      响应头
     * @param sendMessageContext  发送消息上下文
     * @param ctx                 连接上下文
     * @param queueIdInt          队列id
     * @return
     */
    private CompletableFuture<RemotingCommand> handlePutMessageResultFuture(CompletableFuture<PutMessageResult> putMessageResult,
                                                                            RemotingCommand response,
                                                                            RemotingCommand request,
                                                                            MessageExt msgInner,
                                                                            SendMessageResponseHeader responseHeader,
                                                                            SendMessageContext sendMessageContext,
                                                                            ChannelHandlerContext ctx,
                                                                            int queueIdInt) {
        /**
         * 阻塞，当从存放消息完毕时，执行后续操作（即执行handlePutMessageResult方法）
         */
        return putMessageResult.thenApply(r ->
                handlePutMessageResult(r, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt)
        );
    }

    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
                                      RemotingCommand request,
                                      MessageExt msg, TopicConfig topicConfig) {
        String newTopic = requestHeader.getTopic();
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark(
                        "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal() && requestHeader.getMaxReconsumeTimes() != null) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
            if (reconsumeTimes >= maxReconsumeTimes) {
                newTopic = MixAll.getDLQTopic(groupName);
                int queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % DLQ_NUMS_PER_GROUP;
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                        DLQ_NUMS_PER_GROUP,
                        PermName.PERM_WRITE | PermName.PERM_READ, 0
                );
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);
                msg.setDelayTimeLevel(0);
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext sendMessageContext,
                                        final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendMessage request command, {}", request);

        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return response;
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = null;
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(traFlag)
                && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return response;
            }
            putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }

        return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);

    }

    /**
     * 处理存放消息的结果
     * @param putMessageResult      存放结果
     * @param response              响应对象
     * @param request               请求对象
     * @param msg                   内部消息对象
     * @param responseHeader        响应头
     * @param sendMessageContext    发送消息上下文
     * @param ctx                   连接上下文
     * @param queueIdInt            队列id（queueId）
     * @return
     */
    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        // 结果为null，则直接返回系统异常
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;
        // 解析存放消息状态码，转换为对应的响应码
        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("the message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                        this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now. It may be caused by one of the following reasons: " +
                                "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case LMQ_CONSUME_QUEUE_NUM_EXCEEDED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        // 如果发送成功
        if (sendOK) {
            // 如果topic是SCHEDULE_TOPIC_XXXX（即延迟消息的topic）
            if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic())) {
                // 增加统计计数
                this.brokerController.getBrokerStatsManager().incQueuePutNums(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                this.brokerController.getBrokerStatsManager().incQueuePutSize(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
            }
            // 增加统计计数
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            response.setRemark(null);
            // 设置响应头中的msgId（即broker生成的offsetMsgId属性）
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            // 消息队列Id
            responseHeader.setQueueId(queueIdInt);
            // 消息逻辑偏移量
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
            //如果有发送消息的钩子，则执行
            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            return response;
        } else {
            // 如果有发送消息的钩子，则执行
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    private CompletableFuture<RemotingCommand> asyncSendBatchMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                     SendMessageContext mqtraceContext,
                                                                     SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = preSend(ctx, request, requestHeader);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return CompletableFuture.completedFuture(response);
        }

        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
        return handlePutMessageResultFuture(putMessageResult, response, request, messageExtBatch, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    @Override
    public SocketAddress getStoreHost() {
        return storeHost;
    }

    private String diskUtil() {
        double physicRatio = 100;
        String storePath;
        MessageStore messageStore = this.brokerController.getMessageStore();
        if (messageStore instanceof DefaultMessageStore) {
            storePath = ((DefaultMessageStore) messageStore).getStorePathPhysic();
        } else {
            storePath = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        }
        String[] paths = storePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        for (String storePathPhysic : paths) {
            physicRatio = Math.min(physicRatio, UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic));
        }

        String storePathLogis =
                StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
                StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    static private ConsumeMessageContext buildConsumeMessageContext(String namespace,
                                                                    ConsumerSendMsgBackRequestHeader requestHeader,
                                                                    RemotingCommand request) {
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setNamespace(namespace);
        context.setConsumerGroup(requestHeader.getGroup());
        context.setTopic(requestHeader.getOriginTopic());
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));
        return context;
    }

    /**
     * 随机写队列
     * @param writeQueueNums
     * @return
     */
    private int randomQueueId(int writeQueueNums) {
        return ThreadLocalRandom.current().nextInt(99999999) % writeQueueNums;
    }

    /**
     *  准备响应数据
     * @param ctx
     * @param request
     * @param requestHeader
     * @return
     */
    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request,
                                    SendMessageRequestHeader requestHeader) {
        // 创建响应命令对象
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        // 设置唯一id为请求id
        response.setOpaque(request.getOpaque());
        //添加扩展字段属性"MSG_REGION"、"TRACE_ON"
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);
        // 获取配置的broker处理请求的起始服务时间，默认为0
        final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        // 如果当前时间小于起始时间，则broker会返回一个SYSTEM_ERROR，表示现在broker还不能提供服务
        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }
        // 设置code为-1
        response.setCode(-1);
        /**
         * 消息校验，包括自动创建topic的逻辑
         */
        super.msgCheck(ctx, requestHeader, response);

        return response;
    }
}
