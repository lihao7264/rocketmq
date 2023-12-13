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
import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 机房平均分配策略。
 * 消费者只消费绑定的机房中的broker，并对绑定机房中的MessageQueue进行负载均衡。
 * Computer room Hashing queue algorithm, such as Alipay logic room
 */
public class AllocateMessageQueueByMachineRoom extends AbstractAllocateMessageQueueStrategy {
    /**
     * 指定消费的机房名集合
     */
    private Set<String> consumeridcs;
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
        //参数校验
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }
        // 索引（同机房）
        List<MessageQueue> premqAll = new ArrayList<MessageQueue>();
        for (MessageQueue mq : mqAll) {
            String[] temp = mq.getBrokerName().split("@");
            // 如果brokerName符合“机房名@brokerName”的格式要求 且 当前消费者的consumeridcs包含该机房，则加入集合
            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
                premqAll.add(mq);
            }
        }
        // 平均分配的队列
        int mod = premqAll.size() / cidAll.size();
        // 取模剩余的队列
        int rem = premqAll.size() % cidAll.size();
        // 分配队列
        int startIndex = mod * currentIndex;
        int endIndex = startIndex + mod;
        for (int i = startIndex; i < endIndex; i++) {
            result.add(premqAll.get(i));
        }
        // 多加一个队列
        if (rem > currentIndex) {
            result.add(premqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM";
    }

    public Set<String> getConsumeridcs() {
        return consumeridcs;
    }

    public void setConsumeridcs(Set<String> consumeridcs) {
        this.consumeridcs = consumeridcs;
    }
}
