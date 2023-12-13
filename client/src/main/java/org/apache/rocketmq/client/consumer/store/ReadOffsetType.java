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
package org.apache.rocketmq.client.consumer.store;

/**
 * 读取消费点位的三种类型
 */
public enum ReadOffsetType {
    /**
     * 仅从本地内存offsetTable读取
     */
    READ_FROM_MEMORY,
    /**
     * 仅从broker中读取
     */
    READ_FROM_STORE,
    /**
     * 先从本地内存offsetTable读取，读不到再从远程broker中读取。
     * 当出现异常 或 在本地 或 broker未找到对于消费者组的offset记录，则算作第一次启动该消费者组，返回-1。
     */
    MEMORY_FIRST_THEN_STORE;
}
