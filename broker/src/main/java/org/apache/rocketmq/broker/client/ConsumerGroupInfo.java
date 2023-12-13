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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费分组信息
 */
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 消费分组名
     */
    private final String groupName;
    /**
     * <topic,订阅信息>
     */
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
        new ConcurrentHashMap<String, SubscriptionData>();
    /**
     *  <连接,客户端连接信息>
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
        new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    /**
     * 消费类型
     *  push/pull
     */
    private volatile ConsumeType consumeType;
    /**
     * 消费模式
     * 集群/广播
     */
    private volatile MessageModel messageModel;
    /**
     * 启动消费位置
     */
    private volatile ConsumeFromWhere consumeFromWhere;
    /**
     * 订阅信息最新更新时间戳
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * 更新连接
     * @param infoNew           新连接信息
     * @param consumeType       消费类型：PULL or PUSH
     * @param messageModel      消息模式：集群 or 广播
     * @param consumeFromWhere  启动消费位置
     * @return  是否通知
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
        MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        // 更新信息
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
        // 根据当前连接获取channelInfoTable缓存中的连接信息
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        // 如果缓存中的连接信息为null，说明当前连接是一个新连接
        if (null == infoOld) {
            // 存入缓存
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            // 没有该连接信息，则表示有新的consumer连接到此broker，需通知
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {
            // 异常情况
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                    this.groupName,
                    infoOld.toString(),
                    infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }
        // 更新更新时间
        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 更新订阅信息
     * @param subList  订阅信息集合
     * @return
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;
        // 遍历订阅信息集合
        for (SubscriptionData sub : subList) {
            // 根据订阅的topic在ConsumerGroup的subscriptionTable缓存中此前的订阅信息
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            // 如果此前没有关于该topic的订阅信息，则表示此topic为新增订阅
            if (old == null) {
                // 存入subscriptionTable
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                // 此前没有关于该topic的订阅信息，则表示此topic为新增订阅，需通知
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                        this.groupName,
                        sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                // 更新数据（push）
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                        this.groupName,
                        old.toString(),
                        sub.toString()
                    );
                }

                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }
        /**
         * 遍历ConsumerGroup的subscriptionTable缓存
         */
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            // 获取此前订阅的topic
            String oldTopic = next.getKey();

            boolean exist = false;
            // 判断当前的subList是否存在该topic的订阅信息
            for (SubscriptionData sub : subList) {
                // 如果存在，则退出循环
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }
            // 当前的subList不存在该topic的订阅信息，说明consumer移除了对于该topic的订阅
            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                    this.groupName,
                    oldTopic,
                    next.getValue().toString()
                );
                // 移除数据
                it.remove();
                // 需通知
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
