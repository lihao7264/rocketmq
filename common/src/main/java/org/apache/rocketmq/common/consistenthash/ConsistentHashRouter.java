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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性hash算法（一致性hash环）
 *
 * To hash Node objects to a hash ring with a certain amount of virtual node.
 * Method routeNode will return a Node instance which the object key should be allocated to according to consistent hash
 * algorithm
 */
public class ConsistentHashRouter<T extends Node> {
    /**
     * 一致性hash环
     * <虚拟节点的hash值,虚拟节点>
     */
    private final SortedMap<Long, VirtualNode<T>> ring = new TreeMap<Long, VirtualNode<T>>();
    /**
     * hash函数
     */
    private final HashFunction hashFunction;

    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount) {
        this(pNodes, vNodeCount, new MD5Hash());
    }

    /**
     * @param pNodes collections of physical nodes
     * @param vNodeCount amounts of virtual nodes
     * @param hashFunction hash Function to hash Node instances
     */
    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount, HashFunction hashFunction) {
        if (hashFunction == null) {
            throw new NullPointerException("Hash Function is null");
        }
        this.hashFunction = hashFunction;
        if (pNodes != null) {
            for (T pNode : pNodes) {
                // 注册物理节点 及 对应的虚拟节点
                addNode(pNode, vNodeCount);
            }
        }
    }

    /**
     * 注册物理节点 及 对应的虚拟节点
     *  将 pNode 加入 一致性hash环中（即给对应的物理节点创建对应数量的虚拟节点）
     *
     * @param pNode 物理节点
     * @param vNodeCount 虚拟节点数
     */
    public void addNode(T pNode, int vNodeCount) {
        if (vNodeCount < 0)
            throw new IllegalArgumentException("illegal virtual node counts :" + vNodeCount);
        int existingReplicas = getExistingReplicas(pNode);
        // 创建虚拟节点
        for (int i = 0; i < vNodeCount; i++) {
            VirtualNode<T> vNode = new VirtualNode<T>(pNode, i + existingReplicas);
            ring.put(hashFunction.hash(vNode.getKey()), vNode);
        }
    }

    /**
     * 从 一致性hash环 中删除某个物理节点的所有虚拟节点
     */
    public void removeNode(T pNode) {
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            VirtualNode<T> virtualNode = ring.get(key);
            if (virtualNode.isVirtualNodeOf(pNode)) {
                it.remove();
            }
        }
    }

    /**
     * 找到对象 Key 对应的物理节点
     *
     * @param objectKey 对象的 key
     * @return 对应的物理节点
     */
    public T routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        // 计算hash值
        Long hashVal = hashFunction.hash(objectKey);
        SortedMap<Long, VirtualNode<T>> tailMap = ring.tailMap(hashVal);
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode();
    }

    /**
     * 找到某个物理节点的虚拟节点数
     * @param pNode 物理机节点
     * @return 虚拟节点数
     */
    public int getExistingReplicas(T pNode) {
        int replicas = 0;
        for (VirtualNode<T> vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }

    //default hash function
    private static class MD5Hash implements HashFunction {
        MessageDigest instance;

        public MD5Hash() {
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
            }
        }

        @Override
        public long hash(String key) {
            instance.reset();
            instance.update(key.getBytes());
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }

}
