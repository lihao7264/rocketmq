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
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.common.consistenthash.ConsistentHashRouter;
import org.apache.rocketmq.common.consistenthash.HashFunction;
import org.apache.rocketmq.common.consistenthash.Node;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 一致性哈希分配策略。
 * 基于一致性哈希算法分配。
 * Consistent Hashing queue algorithm
 */
public class AllocateMessageQueueConsistentHash extends AbstractAllocateMessageQueueStrategy {

    /**
     * 物理节点的虚拟节点的数量，不可小于0，默认10。
     */
    private final int virtualNodeCnt;

    /**
     * 自定义的哈希函数，默认为MD5Hash。
     */
    private final HashFunction customHashFunction;

    public AllocateMessageQueueConsistentHash() {
        this(10);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt) {
        this(virtualNodeCnt, null);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt, HashFunction customHashFunction) {
        if (virtualNodeCnt < 0) {
            throw new IllegalArgumentException("illegal virtualNodeCnt :" + virtualNodeCnt);
        }
        this.virtualNodeCnt = virtualNodeCnt;
        this.customHashFunction = customHashFunction;
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
        // 包装为ClientNode对象
        Collection<ClientNode> cidNodes = new ArrayList<ClientNode>();
        for (String cid : cidAll) {
            cidNodes.add(new ClientNode(cid));
        }

        /**
         * 实例化ConsistentHashRouter对象，用于产生虚拟节点、构建哈希环
         * 如果未指定哈希函数，则采用MD5Hash作为哈希函数
         */
        final ConsistentHashRouter<ClientNode> router; //for building hash ring
        if (customHashFunction != null) {
            router = new ConsistentHashRouter<ClientNode>(cidNodes, virtualNodeCnt, customHashFunction);
        } else {
            router = new ConsistentHashRouter<ClientNode>(cidNodes, virtualNodeCnt);
        }

        List<MessageQueue> results = new ArrayList<MessageQueue>();
        /**
         * 遍历消息队列集合
         */
        for (MessageQueue mq : mqAll) {
            // 对messageQueue进行hash计算，按顺时针找到最近的consumer节点
            ClientNode clientNode = router.routeNode(mq.toString());
            // 如果是当前consumer，则加入结果集
            if (clientNode != null && currentCID.equals(clientNode.getKey())) {
                results.add(mq);
            }
        }

        return results;

    }

    @Override
    public String getName() {
        return "CONSISTENT_HASH";
    }

    private static class ClientNode implements Node {
        /**
         * 客户端id
         */
        private final String clientID;

        public ClientNode(String clientID) {
            this.clientID = clientID;
        }

        @Override
        public String getKey() {
            return clientID;
        }
    }

}
