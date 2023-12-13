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
package org.apache.rocketmq.client.producer;
// 描述本地事务执行状态
public enum LocalTransactionState {
    /**
     * 本地事务执行成功
     * 需要提交半事务消息
     */
    COMMIT_MESSAGE,
    /**
     * 本地事务执行失败
     * 需要回滚半事务消息
     */
    ROLLBACK_MESSAGE,
    /**
     * 不确定，表示需进行回查以确定本地事务的执行结果
     */
    UNKNOW,
}
