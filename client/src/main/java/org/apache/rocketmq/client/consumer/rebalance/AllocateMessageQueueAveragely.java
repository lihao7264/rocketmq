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
 * （默认）平均分配策略
 *  尽量将消息队列平均分配给所有消费者，多余的队列分配至排在前面的消费者。
 *  分配时，前一个消费者分配完了，才给下一个消费者分配。
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

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
        // 当前currentCID（客户端id）在集合中的索引位置
        int index = cidAll.indexOf(currentCID);
        /**
         * 计算平均分配后的余数
         * 大于0：表示不能被整除，必然有些消费者会多分配一个队列，有些消费者少分配一个队列
         *
         */
        int mod = mqAll.size() % cidAll.size();
        /**
         * 计算当前消费者分配的队列数
         *  1、如果队列数小于等于消费者数，则每个消费者最多只能分到一个队列，则算作1（后续还会计算）；否则表示每个消费者至少分配一个队列，需继续计算
         *  2、如果mod大于0 且 当前消费者索引小于mod，则当前消费者分到的队列数为平均分配的队列数+1；否则分到的队列数为平均分配的队列数（即索引在余数范围内的，多分配一个队列）
         */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        /**
         * 如果mod大于0 且 当前消费者索引小于mod，则起始索引为index * averageSize；否则起始索引为index * averageSize + mod
         */
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        /**
         * 最终分配的消息队列数。
         * 取最小值的原因：有些队列会分配至较少的队列 甚至 无法分配到队列
         */
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        /**
         *  分配队列，按照顺序分配
         */
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
