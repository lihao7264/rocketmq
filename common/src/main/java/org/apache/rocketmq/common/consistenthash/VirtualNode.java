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
package org.apache.rocketmq.common.consistenthash;

/**
 * 虚拟节点
 * @param <T>
 */
public class VirtualNode<T extends Node> implements Node {
    /**
     * 虚拟节点所属的物理节点
     */
    final T physicalNode;

    /**
     * 虚拟节点下标（即物理节点对应的第几个虚拟节点）
     */
    final int replicaIndex;

    public VirtualNode(T physicalNode, int replicaIndex) {
        this.replicaIndex = replicaIndex;
        this.physicalNode = physicalNode;
    }

    @Override
    public String getKey() {
        // 客户端id-
        return physicalNode.getKey() + "-" + replicaIndex;
    }

    /**
     * 虚拟节点是否所属该物理节点
     * @param pNode
     * @return
     */
    public boolean isVirtualNodeOf(T pNode) {
        return physicalNode.getKey().equals(pNode.getKey());
    }

    public T getPhysicalNode() {
        return physicalNode;
    }
}
