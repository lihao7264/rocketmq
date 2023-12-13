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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 机房就近分配策略:基于机房近端优先级的代理分配策略。
 * 消费者对绑定机房中的MessageQueue进行负载均衡（如果任何消费者在机房中活动，则部署在同一台机器中的代理的消息队列应仅分配给这些消费者）。
 * 此外，对于某些拥有消息队列但却没有消费者的机房的消息队列会被所有消费者分配（这些消息队列可与所有消费者共享，因为没有活跃的消费者来消费它们）
 * 具体的分配策略：另外传入的一个AllocateMessageQueueStrategy的实现。
 */
public class AllocateMachineRoomNearby extends AbstractAllocateMessageQueueStrategy {
    /**
     * 用于真正分配消息队列的策略对象
     */
    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;//actual allocate strategy
    /**
     * 机房解析器，从clientID和brokerName中解析出机房名称
     */
    private final MachineRoomResolver machineRoomResolver;

    public AllocateMachineRoomNearby(AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MachineRoomResolver machineRoomResolver) throws NullPointerException {
        if (allocateMessageQueueStrategy == null) {
            throw new NullPointerException("allocateMessageQueueStrategy is null");
        }

        if (machineRoomResolver == null) {
            throw new NullPointerException("machineRoomResolver is null");
        }

        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.machineRoomResolver = machineRoomResolver;
    }
    /**
     * 分配队列方法
     * @param consumerGroup 当前consumerGroup
     * @param currentCID 当前currentCID
     * @param mqAll     当前topic的mq，已排序
     * @param cidAll 当前consumerGroup的clientId集合，已排序
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        //group mq by machine room
        // 将消息队列根据机房分组：<机房，消息队列集合>
        Map<String/*machine room */, List<MessageQueue>> mr2Mq = new TreeMap<String, List<MessageQueue>>();
        for (MessageQueue mq : mqAll) {
            // 获取broker所属机房
            String brokerMachineRoom = machineRoomResolver.brokerDeployIn(mq);
            if (StringUtils.isNoneEmpty(brokerMachineRoom)) {
                if (mr2Mq.get(brokerMachineRoom) == null) {
                    // 存入map
                    mr2Mq.put(brokerMachineRoom, new ArrayList<MessageQueue>());
                }
                // 添加消息队列
                mr2Mq.get(brokerMachineRoom).add(mq);
            } else {
                throw new IllegalArgumentException("Machine room is null for mq " + mq);
            }
        }

        //group consumer by machine room
        // 将消费者根据机房分组
        Map<String/*machine room */, List<String/*clientId*/>> mr2c = new TreeMap<String, List<String>>();
        for (String cid : cidAll) {
            // 获取消费者所属的机房
            String consumerMachineRoom = machineRoomResolver.consumerDeployIn(cid);
            if (StringUtils.isNoneEmpty(consumerMachineRoom)) {
                if (mr2c.get(consumerMachineRoom) == null) {
                    // 存入map
                    mr2c.put(consumerMachineRoom, new ArrayList<String>());
                }
                // 添加消费者
                mr2c.get(consumerMachineRoom).add(cid);
            } else {
                throw new IllegalArgumentException("Machine room is null for consumer id " + cid);
            }
        }

        List<MessageQueue> allocateResults = new ArrayList<MessageQueue>();

        //1.allocate the mq that deploy in the same machine room with the current consumer
        /**
         * 分配部署在与当前消费者相同的机房中的mq
         */
        // 获取当前消费者的机房
        String currentMachineRoom = machineRoomResolver.consumerDeployIn(currentCID);
        // 移除并获取当前消费者的机房的队列集合
        List<MessageQueue> mqInThisMachineRoom = mr2Mq.remove(currentMachineRoom);
        // 获取当前消费者的机房的消费者集合
        List<String> consumerInThisMachineRoom = mr2c.get(currentMachineRoom);
        if (mqInThisMachineRoom != null && !mqInThisMachineRoom.isEmpty()) {
            /**
             * 调用传入的分配策略，对mqInThisMachineRoom和consumerInThisMachineRoom进行分配
             */
            allocateResults.addAll(allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqInThisMachineRoom, consumerInThisMachineRoom));
        }
        /**
         * 如果机房中没有的消费者，则将剩余的mq分配给每个机房
         */
        //2.allocate the rest mq to each machine room if there are no consumer alive in that machine room
        for (Entry<String, List<MessageQueue>> machineRoomEntry : mr2Mq.entrySet()) {
            // 如果某个拥有消息队列的机房没有对应的消费者，则它的消息队列由当前所有的消费者分配
            if (!mr2c.containsKey(machineRoomEntry.getKey())) { // no alive consumer in the corresponding machine room, so all consumers share these queues
                allocateResults.addAll(allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, machineRoomEntry.getValue(), cidAll));
            }
        }

        return allocateResults;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM_NEARBY" + "-" + allocateMessageQueueStrategy.getName();
    }

    /**
     * 机房解析器，从clientID和brokerName中解析出机房名称
     * A resolver object to determine which machine room do the message queues or clients are deployed in.
     *
     * AllocateMachineRoomNearby will use the results to group the message queues and clients by machine room.
     *
     * The result returned from the implemented method CANNOT be null.
     */
    public interface MachineRoomResolver {
        /**
         * 获取broker所属机房
         * @param messageQueue
         * @return
         */
        String brokerDeployIn(MessageQueue messageQueue);

        /**
         * 获取消费者所属机房
         * @param clientID
         * @return
         */
        String consumerDeployIn(String clientID);
    }
}
