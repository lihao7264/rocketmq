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
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 环形平均分配策略
 * 将消息队列平均分配给所有消费者，多余的队列分配至排在前面的消费者。
 * 与平均分配策略差不多。
 * 区别：分配时，按照消费者的顺序进行一轮一轮的分配，直到分配完所有消息队列。
 * Cycle average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragelyByCircle extends AbstractAllocateMessageQueueStrategy {

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
        // 参数校验
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        // 索引
        int index = cidAll.indexOf(currentCID);
        // 获取每个分配轮次中属于该消费者的对应的消息队列
        for (int i = index; i < mqAll.size(); i++) {
            if (i % cidAll.size() == index) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG_BY_CIRCLE";
    }
}
