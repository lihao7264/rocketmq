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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * StoreCheckpoint记录着commitLog、ConsumeQueue、Index文件的最后更新时间点
 * 作用：当上一次broker是异常结束时，会根据StoreCheckpoint的数据进行恢复，这决定着文件从哪里开始恢复、删除文件
 */
public class StoreCheckpoint {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 最新commitlog文件的刷盘时间戳，单位毫秒。
     */
    private volatile long physicMsgTimestamp = 0;
    /**
     * 最新consumeQueue文件的刷盘时间戳，单位毫秒。
     */
    private volatile long logicsMsgTimestamp = 0;
    /**
     * 创建最新indexFile文件的时间戳，单位毫秒。
     */
    private volatile long indexMsgTimestamp = 0;

    /**
     * 创建StoreCheckpoint检查点对象
     *  加载checkpoint 检查点文件（文件位置：{storePathRootDir}/checkpoint），创建storeCheckpoint对象。
     * @param scpPath
     * @throws IOException
     */
    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        // 判断存在当前文件
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();
        // 对checkpoint文件执行mmap操作
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        // mmap大小为OS_PAGE_SIZE（即OS一页：4k）
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            // 获取commitlog文件的时间戳（即最新commitlog文件的刷盘时间戳）
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            // 获取consumeQueue文件的时间戳（即最新consumeQueue文件的刷盘时间戳）
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            // 获取index文件的时间戳（即创建最新indexFile文件的时间戳）
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", "
                    + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
        } else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
        this.mappedByteBuffer.force();
    }

    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0)
            min = 0;

        return min;
    }

    public long getIndexMsgTimestamp() {
        return indexMsgTimestamp;
    }

    public void setIndexMsgTimestamp(long indexMsgTimestamp) {
        this.indexMsgTimestamp = indexMsgTimestamp;
    }

}