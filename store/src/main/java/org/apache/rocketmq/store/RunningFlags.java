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

public class RunningFlags {

    /**
     * 不可读标识位
     */
    private static final int NOT_READABLE_BIT = 1;

    /**
     * 不可写标识位
     */
    private static final int NOT_WRITEABLE_BIT = 1 << 1;
    /**
     * 写ConsumeQueue错误标识位
     */
    private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

    /**
     * 写indexFile错误标识位
     */
    private static final int WRITE_INDEX_FILE_ERROR_BIT = 1 << 3;

    /**
     * 磁盘满标识位
     */
    private static final int DISK_FULL_BIT = 1 << 4;

    /**
     * 标识
     * 比如：11111（表示 磁盘满、写indexFile错误、写ConsumeQueue错误、不可写、不可读）
     *
     */
    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    /**
     * 是否可读 且 设置不可读标识为0（可读）
     * 比如：
     *    flagBits为0，result为true，flagBits为0
     *    flagBits为1，result为false，flagBits为0
     * @return
     */
    public boolean getAndMakeReadable() {
        // 是否可读
        boolean result = this.isReadable();
        if (!result) {
            // 如果不可读的话，设置为可读（flagBits的第一位 清 0）
            this.flagBits &= ~NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean isReadable() {
        //  & 不可读标志位，为0，则可读
        if ((this.flagBits & NOT_READABLE_BIT) == 0) {
            return true;
        }

        return false;
    }

    /**
     * 是否可读 且 设置不可读标识为1（不可读）
     * 比如：
     *    flagBits为0，result为true，flagBits为1
     *    flagBits为1，result为false，flagBits为1
     * @return
     */
    public boolean getAndMakeNotReadable() {
        // 是否可读
        boolean result = this.isReadable();
        if (result) {
            // 如果可读的话，则位或运算（第一位），设置为不可读
            this.flagBits |= NOT_READABLE_BIT;
        }
        return result;
    }

    /**
     * 是否可写 且 设置不可写标识为0（可写）
     * 比如：
     *    flagBits为0，result为true，flagBits为0
     *    flagBits为2，result为false，flagBits为0
     * @return
     */
    public boolean getAndMakeWriteable() {
        // 是否可写
        boolean result = this.isWriteable();
        if (!result) {
            // 如果不可写的话，设置为可写（flagBits的第二位 清 0）
            this.flagBits &= ~NOT_WRITEABLE_BIT;
        }
        return result;
    }

    // 是否可写
    public boolean isWriteable() {
        // & 不可写、写ConsumeQueue错误、写IndexFile错误、磁盘满标识位的结果为0（也就是这四种情况都没触发）
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    //for consume queue, just ignore the DISK_FULL_BIT
    // consumerQueue 是否可写
    public boolean isCQWriteable() {
        // & 不可写、写ConsumeQueue错误、写IndexFile错误的结果为0（这三种情况都没触发）
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    /**
     * 是否可写 且 设置不可写标识为1（不可写）
     * 比如：
     *    flagBits为0，result为true，flagBits为2
     *    flagBits为2，result为false，flagBits为2
     * @return
     */
    public boolean getAndMakeNotWriteable() {
        // 是否可写
        boolean result = this.isWriteable();
        if (result) {
            // 如果可写的话，则位或运算（第二位），设置为不可写
            this.flagBits |= NOT_WRITEABLE_BIT;
        }
        return result;
    }

    // 设置写ConsumeQueue错误
    public void makeLogicsQueueError() {
        // 位或运算（第三位）
        this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT;
    }
    // 是否写ConsumeQueue错误
    public boolean isLogicsQueueError() {
        // &第三位即可
        if ((this.flagBits & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT) {
            return true;
        }

        return false;
    }
    // 设置写indexFile错误
    public void makeIndexFileError() {
        // 位或运算（第四位）
        this.flagBits |= WRITE_INDEX_FILE_ERROR_BIT;
    }
    // 是否写indexFile错误
    public boolean isIndexFileError() {
        // &第四位即可
        if ((this.flagBits & WRITE_INDEX_FILE_ERROR_BIT) == WRITE_INDEX_FILE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    /**
     * 磁盘是否刚变满 且 设置磁盘满标识为1（满的）
     * 比如：
     *    flagBits为0，result为true，flagBits为16
     *    flagBits为16，result为false，flagBits为16
     * @return
     */
    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        // 位或运算（第五位）
        this.flagBits |= DISK_FULL_BIT;
        return result;
    }

    /**
     * 磁盘之前是否未满 且 设置磁盘满标识为0（不满了）
     * 比如：
     *    flagBits为0，result为true，flagBits为0
     *    flagBits为16，result为false，flagBits为0
     * @return
     */
    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        // 将磁盘满标识清0（flagBits的第五位 清 0）
        this.flagBits &= ~DISK_FULL_BIT;
        return result;
    }

    public static void main(String[] args) {
        Integer flagBits = 0 ;
//        result |= 1;
//        System.out.println(result);
//        Integer intstr=1 << 4;
//        System.out.println(intstr);

        boolean result = !((flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
//        flagBits |= DISK_FULL_BIT;
        flagBits &= ~DISK_FULL_BIT;
        System.out.println(flagBits);
        System.out.println(result);

    }
}
