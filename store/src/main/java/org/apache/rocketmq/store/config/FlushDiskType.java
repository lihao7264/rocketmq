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
package org.apache.rocketmq.store.config;

/**
 * 两种刷盘策略
 */
public enum FlushDiskType {
    /**
     * 同步刷盘：只有在消息真正持久化至磁盘后RocketMQ的Broker端才会真正返回给Producer端一个成功的ACK响应。
     * 同步刷盘对MQ消息可靠性，是一种不错的保障，但性能上会有较大影响，一般适用于金融业务应用该模式较多。
     */
    SYNC_FLUSH,
    /**
     * 异步刷盘：
     * 能充分利用OS的PageCache的优势，只要消息写入PageCache即可将成功的ACK返回给Producer端。
     * 消息刷盘采用后台异步线程提交的方式进行，降低读写延迟，提高MQ的性能和吞吐量。
     */
    ASYNC_FLUSH
}
