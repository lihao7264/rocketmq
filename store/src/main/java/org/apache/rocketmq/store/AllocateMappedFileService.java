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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * 创建 MappedFile文件服务：用于初始化MappedFile和预热MappedFile
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 创建文件最多等待5s
     */
    private static int waitTimeOut = 1000 * 5;
    /**
     * <文件路径,分配文件请求>
     */
    private ConcurrentMap<String, AllocateRequest> requestTable =
            new ConcurrentHashMap<String, AllocateRequest>();
    /**
     * 分配文件优先级队列
     * 获取优先级最高的一个请求（即文件名最小 或 起始offset最小的请求）
     */
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
            new PriorityBlockingQueue<AllocateRequest>();
    /**
     * 是否发生异常
     */
    private volatile boolean hasException = false;
    /**
     * 所属的消息存储组件
     */
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 添加两个请求到处理任务池，然后阻塞等待异步创建并返回MappedFile
     * @param nextFilePath
     * @param nextNextFilePath
     * @param fileSize    文件大小默认1G
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        // 可以提交的请求
        int canSubmitRequests = 2;
        /**
         * 如果当前节点不是从节点 且 异步刷盘策略 且 transientStorePoolEnable参数配置为true 且 fastFailIfNoBufferInStorePool为true
         * 则重新计算最多可提交几个文件创建请求
         */
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                    && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }
        // 根据nextFilePath创建一个请求对象，并将请求对象存入requestTable map集合中
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
        // 如果存入成功
        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            // 将请求存入requestQueue中
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            // 可以提交的请求数自减
            canSubmitRequests--;
        }
        // 根据nextNextFilePath创建另一个请求对象，并将请求对象存入requestTable map集合中
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                // 将请求存入requestQueue中
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }
        // 有异常，则直接返回
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }
        // 获取此前存入的nextFilePath对应的请求

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                // 同步等待最多5s
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    // 超时
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    // 如果nextFilePath对应的MappedFile创建成功，则从requestTable移除对应的请求
                    this.requestTable.remove(nextFilePath);
                    // 返回创建的mappedFile
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     *  创建mappedFile
     */
    public void run() {
        log.info(this.getServiceName() + " service started");
        /**
         * 死循环
         * 如果服务未停止 且 未被线程中断，则一直循环执行mmapOperation方法
         */
        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Only interrupted by the external thread, will return false
     * mmap 操作，只有被外部线程中断才会返回false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            /**
             * 从requestQueue中获取优先级最高的一个请求（即文件名最小 或 起始offset最小的请求）
             * requestQueue：一个优先级队列
             */
            req = this.requestQueue.take();
            // 从requestTable获取对应的请求
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }
            // 获取对应的mappedFile，如果为null则创建
            if (req.getMappedFile() == null) {
                // 起始时间
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                /**
                 *  如果当前节点不是从节点 且 异步刷盘策略 且 transientStorePoolEnable参数配置为true，则使用堆外内存，默认不使用
                 *  RocketMQ中引入的 transientStorePoolEnable 能缓解 pagecache 的压力。
                 *  原理：基于DirectByteBuffer和MappedByteBuffer的读写分离
                 *       消息先写入DirectByteBuffer（堆外内存），然后从MappedByteBuffer（pageCache）读取。
                 */
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        // 基于SPI机制获取自定义的MappedFile
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        // 初始化
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    // 普通方式创建mappedFile，并进行mmap
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }
                // 创建mappedFile消耗的时间
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }

                /**
                 * 如果mappedFile大小大于等于1G 且 warmMappedFileEnable参数为true，则预写mappedFile（即内存预热或文件预热）
                 * 注意：warmMappedFileEnable参数默认为false（即默认不开启文件预热），因此需手动开启
                 */
                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                        .getMappedFileSizeCommitLog()
                        &&
                        this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    // 预热文件
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            // 如果创建成功，则将请求对象中的countDownLatch释放计数，可唤醒在putRequestAndReturnMappedFile方法中被阻塞的线程
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 分配文件的请求
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        /**
         * 文件路径
         */
        // Full file path
        private String filePath;
        /**
         * 文件大小
         */
        private int fileSize;
        /**
         * 用于等待异步文件创建成功通知
         */
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 对应的MappedFile
         */
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
