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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
// 操作IndexFile文件服务
public class IndexService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     * 尝试创建IndexFile的最大次数
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    /**
     * 消息存储服务
     */
    private final DefaultMessageStore defaultMessageStore;
    /**
     * 每个indexFile文件中，包含最大数量的slot槽
     * 默认值：500w个
     */
    private final int hashSlotNum;
    /**
     * 每个indexFile文件中，包含最大数量的索引单元
     * 默认值：2000w个
     */
    private final int indexNum;
    /**
     * IndexFile的存储路径：{storePathRootDir}/index
     */
    private final String storePath;
    /**
     * IndexFile集合
     */
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        /**
         * 每个indexFile文件中，包含最大数量的slot槽
         * 默认值：500w个
         */
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        /**
         * 每个indexFile文件中，包含最大数量的索引单元
         * 默认值：2000w个
         */
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // 存储路径：{storePathRootDir}/index
        this.storePath =
                StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 加载 index 索引文件，目录路径为{storePathRootDir}/index
     * @param lastExitOK 上次是否正常退出
     */
    public boolean load(final boolean lastExitOK) {
        // 获取上级目录路径，{storePathRootDir}/index
        File dir = new File(this.storePath);
        // 获取内部的index索引文件
        File[] files = dir.listFiles();
        if (files != null) {
            // 按照文件名字中的时间戳排序
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    // 一个index文件对应着一个IndexFile实例
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // 加载index文件
                    f.load();
                    // 如果上一次是异常推出，且当前index文件中最后一个消息的落盘时间戳大于最后一个index索引文件创建时间，则该索引文件被删除
                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                                .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    // 加入到索引文件集合
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    /**
     * IndexService#deleteExpiredFile(long)
     * 删除消息CommitLog偏移量offset之前的所有IndexFile文件
     *
     * @param offset 当前CommitLog的最小偏移量
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            // 读锁
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }
            /**
             *  比较第一个 IndexFile 的最大 offset 与 当前CommitLog的最小偏移量
             *   如果大于 IndexFile 的最大 offset，说明无需删除任何文件
             *   如果小于 IndexFile 的最大 offset，说明需要删除部分文件
             */
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            // 解锁
            this.readWriteLock.readLock().unlock();
        }
        // 有文件需被删除，遍历所有IndexFile文件，删除所有最大 offset 小于 当前CommitLog的最小偏移量 的文件
        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    // 删除文件末尾的offset 小于 当前CommitLog的最小偏移量 的文件
                    fileList.add(f);
                } else {
                    break;
                }
            }
            // 删除文件
            this.deleteExpiredFile(fileList);
        }
    }

    /**
     * IndexService#deleteExpiredFile(java.util.List)
     * @param files 需销毁的文件集合
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                // 加写锁
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    // 销毁文件（上次关闭时间间隔超过30s则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间））
                    boolean destroyed = file.destroy(3000);
                    // 销毁成功的话，则从indexFileList移除该IndexFile
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                // 释放写锁
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    /**
     *  构建key
     * @param topic
     * @param key
     * @return
     */
    private String buildKey(final String topic, final String key) {
        // 拼接
        return topic + "#" + key;
    }

    /**
     * 构建Index索引
     * @param req
     */
    public void buildIndex(DispatchRequest req) {
        /**
         * 获取或创建最新索引文件，支持重试最多3次
         */
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            // 获取结束物理索引
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            // 获取topic和keys
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            /**
             * 如果消息在commitlog中的偏移量小于该文件的结束索引在commitlog中的偏移量，则表示已为该消息之后的消息构建Index索引
             * 此时直接返回，无需创建索引
             */
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }
            // 获取该消息的事务类型
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                // 如果是事务回滚消息，则直接返回，无需创建索引
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            /**
             * 获取客户端生成的uniqId（即msgId），从逻辑上表示客户端生成的唯一一条消息
             * 如果uniqId不为null，则为uniqId构建索引
             */
            if (req.getUniqKey() != null) {
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }
            /**
             * 获取客户端传递的keys
             * 如果keys不为空，则为keys中的每一个key构建索引
             */
            if (keys != null && keys.length() > 0) {
                // 按照空格拆分key
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                // 为keys中的每一个key构建索引
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * 构建Index索引
     * @param indexFile  indexFile
     * @param msg     消息
     * @param idxKey  key
     * @return
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        // 循环尝试构建Index索引
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");
            // 构建失败，则尝试获取或创建最新索引文件，支持重试
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }
            // 再次尝试构建Index索引
            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * 获取或创建最新索引文件，支持重试最多3次
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;
        // 循环尝试，尝试创建索引文件的最大次数为3
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // 获取最新的索引文件，如果文件写满了 或 还没有文件则会自动创建新的索引文件
            indexFile = this.getAndCreateLastIndexFile();
            // 如果获取的indexFile不为null，则退出循环
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        // 标记indexFile异常
        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取最新的索引文件，如果文件写满了 或 还没有文件则会自动创建新的索引文件
     * @return
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;
        /**
         * 尝试获取最新IndexFile
         */
        {
            // 尝试获取读锁
            this.readWriteLock.readLock().lock();
            // 如果indexFileList不为空
            if (!this.indexFileList.isEmpty()) {
                // 尝试获取最后一个IndexFile
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    // 如果最后一个IndexFile未写满，则赋值给indexFile
                    indexFile = tmp;
                } else {
                    /**
                     * 如果最后一个IndexFile写满了，则创建新文件
                     * 获取目前最后一个文件的endPhyOffset
                     */
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    // 获取目前最后一个文件的endTimestamp
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    // 赋值给prevIndexFile
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }
        /**
         * 尝试创建一个新的IndexFile
         */
        if (indexFile == null) {
            try {
                // 获取完整文件名{$ROCKETMQ_HOME}/store/index${fileName}，fileName是以创建时的时间戳命名的，精确到毫秒
                String fileName =
                        this.storePath + File.separator
                                + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 创建IndexFile
                indexFile =
                        new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                                lastUpdateIndexTimestamp);
                // 获取写锁
                this.readWriteLock.writeLock().lock();
                // 加入到indexFileList集合中
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                // 释放写锁
                this.readWriteLock.writeLock().unlock();
            }
            /**
             * 创建了新文件后，尝试将上一个文件刷盘
             */
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                /**
                 * 新开一个线程，异步对上一个IndexFile文件刷盘
                 */
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    /**
     * 刷盘IndexFile
     * @param f
     */
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;
        // 如果满了，则写IndexFile的最新时间未最后一个消息的时间
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
