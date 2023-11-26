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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile作为一个RocketMQ的物理文件在Java中的映射类
 *    commitLog、consumerQueue、indexFile 3种文件磁盘的读写都通过MappedFile操作。
 */
public class MappedFile extends ReferenceResource {
    // 操作系统的每页大小（4K）
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 当前JVM实例中MappedFile虚拟内存总大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    // 当前JVM实例中MappedFile对象（mmap）个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 写入指针：当前写文件位置（当前文件所映射到的消息写入page cache的位置）
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 提交指针：提交位置（已提交的最新位置）
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 刷写指针：刷盘位置（刷盘的最新位置）
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 单个文件的大小
    protected int fileSize;
    // 文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    /**
     * 写入buffer
     *    如果开启了 transientStorePoolEnable 时不为空，writeBuffer 使用堆外内存。
     *    消息先进入到堆外内存中，然后通过Commit线程提交到内存映射buffer中，再通过Flush线程将数据持久化到磁盘中
     */
    protected ByteBuffer writeBuffer = null;
    // writeBuffer 池（暂存池）：只有在开启 transientStorePoolEnable 时生效，默认为5个
    protected TransientStorePool transientStorePool = null;
    // 文件名
    private String fileName;
    /**
     * 文件映射的初始物理偏移量
     *  CommitLog、ConsumeQueue的文件初始物理偏移量 和 文件名相同
     */
    private long fileFromOffset;
    // 映射文件
    private File file;
    /**
     * 内存映射对象：操作系统的 PageCache
     * 把commitlog文件完全的映射到虚拟内存（内存映射：mmap，提升读写性能）
     */
    private MappedByteBuffer mappedByteBuffer;
    // 最后一条消息保存时间
    private volatile long storeTimestamp = 0;
    // 是否MappedFileQueue队列中第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 创建MappedFile并映射文件
     * 采用mmap
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        // 调用init初始化
        init(fileName, fileSize);
    }

    /**
     * 采用堆外内存
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void  createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    /**
     * MappedFile#clean(java.nio.ByteBuffer)
     * 清理ByteBuffer（对内存映射区域进行清理）
     * 仅用于 销毁direct类型的buffer（直接内存缓冲）
     * @param buffer
     */
    public static void clean(final ByteBuffer buffer) {
        // 如果 内存映射区域是null 或 不是堆外内存 或 内存映射区域容量是0，则此时不能清理
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        /**
         * 正常清理：利用ByteBuffer的cleaner的clean方法销毁ByteBuffer
         * 获取到原生buffer -> 通过反射调用buffer里面的cleaner函数 获取到 原生buffer绑定的清理组件
         * -> 获取到原生buffer的清理组件后，对清理组件反射调用clean函数，对原生buffer做一个清理。
         */
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    /**
     * MappedFile#invoke(java.lang.Object, java.lang.String, java.lang.Class[])
     * 安全反射
     * @param target     反射目标对象
     * @param methodName   反射方法名称
     * @param args   反射方法入参
     * @return
     */
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        // JDK中提供的安全调用机制
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                // 反射执行对应的方法
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * 获取反射的方法
     * @param target
     * @param methodName
     * @param args
     * @return
     * @throws NoSuchMethodException
     */
    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /**
     * 对内存映射区域需去获取一个视图buffer（原生buffer）
     * 解决原生buffer被一些包装性的一些buffer进行包裹，需一层一层的剥开buffer，获取一个原生buffer
     * @param buffer
     * @return
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
        // 1、获取要执行的ByteBuffer的方法的方法名（默认反射获取viewedBuffer方法）
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            // 1.1、如果ByteBuffer的方法中存在attachment方法，则调用attachment方法
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }
        // 2、反射调用 ByteBuffer的viewedBuffer/attachment 方法 获取到了一个内部的视图buffer
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)

            return buffer;
        else
            // 如果该视图buffer不是null，则递归调用视图buffer获取函数
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 采用堆外内存
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        // 普通初始化
        init(fileName, fileSize);
        // 设置写buffer，采用堆外内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 创建MappedFile并映射文件
     * @param fileName  文件名
     * @param fileSize  文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        /**
         * 文件名
         *  CommitLog：长度为20位，左边补零，剩余为起始偏移量
         *             举例：00000000000000000000表示第一个文件，起始偏移量为0
         */
        this.fileName = fileName;
        /**
         * 文件大小：
         *  CommitLog大小默认值：1G=1073741824
         */
        this.fileSize = fileSize;
        // 构建file对象
        this.file = new File(fileName);
        // 构建文件起始索引（取自文件名）
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 确保文件目录存在
        ensureDirOK(this.file.getParent());

        try {
            // 对当前commitlog文件构建文件通道fileChannel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 把commitlog文件完全的映射到虚拟内存（内存映射：mmap，提升读写性能）
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 记录数据
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            // 释放fileChannel，注意释放fileChannel不会对之前的mappedByteBuffer映射产生影响
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 追加消息
     * @param msg      消息
     * @param cb       回调函数
     * @param putMessageContext   存放消息上下文
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
                                             PutMessageContext putMessageContext) {
        // 调用appendMessagesInner方法
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
                                              PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    /**
     * 追加消息
     * @param messageExt        消息
     * @param cb                回调函数
     * @param putMessageContext  存放消息上下文
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
                                                   PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        // 获取写入指针的位置
        int currentPos = this.wrotePosition.get();
        // 如果小于文件大小，则可以写入
        if (currentPos < this.fileSize) {
            // 如果存在writeBuffer（即支持堆外缓存），则使用writeBuffer进行读写分离，否则使用mmap方式写
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 设置写入位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            /**
             * 通过回调函数执行实际写入
             */
            if (messageExt instanceof MessageExtBrokerInner) {
                // 单条消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBatch) {
                // 批量消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新写指针的位置
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 更新存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 追加消息
     * 空间足够的话,将data[]直接写入fileChannel
     * 更新wrotePosition
     * @param data  追加的数据
     * @return
     */
    public boolean appendMessage(final byte[] data) {
        // 获取写入位置
        int currentPos = this.wrotePosition.get();
        // 如果当前写文件位置加上消息大小（数据长度）<= 文件最大大小，则将消息写入mappedByteBuffer（即空间足够）
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 消息写入mappedByteBuffer即可，并未执行刷盘
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写入位置
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    /**
     * MappedFile#appendMessage(byte[], int, int)
     * 追加信息：空间足够的话,将data[]的一部分写入fileChannel
     * @param data     需写入的数据
     * @param offset   要使用数组的偏移量
     * @param length   数组字节长度
     * @return
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        // 当前写文件位置
        int currentPos = this.wrotePosition.get();
        // 如果可以保存的下对应的byte[]的数据，则进行保存，否则失败
        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 新的写文件位置=当前写文件位置+数据长度
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘
     * @param flushLeastPages 最少刷盘的页数
     * @return The current flushed position  当前刷盘的位置
     */
    public int flush(final int flushLeastPages) {
        /**
         * 判断是否可以刷盘
         * 会刷盘的三种情况：
         *   情况一：如果文件已满
         *   情况二：如果flushLeastPages大于0 且 脏页数大于等于flushLeastPages
         *   情况三：如果flushLeastPages等于0 且 存在脏数据
         */
        if (this.isAbleToFlush(flushLeastPages)) {
            // 增加对该MappedFile的引用次数
            if (this.hold()) {
                // 获取写入位置
                int value = getReadPosition();

                try {
                    /**
                     * 只将数据追加到fileChannel或mappedByteBuffer中，不会同时追加到这两个中。
                     *  如果使用了堆外内存，则通过fileChannel强制刷盘（异步堆外内存走的逻辑）
                     */
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 如果使用了堆外内存，则通过fileChannel强制刷盘（异步堆外内存走的逻辑）
                        this.fileChannel.force(false);
                    } else {
                        // 如果未使用堆外内存，则通过mappedByteBuffer强制刷盘，这是同步 或 异步刷盘走的逻辑
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 设置刷盘位置为写入位置
                this.flushedPosition.set(value);
                // 减少对该MappedFile的引用次数
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        // 获取最新刷盘位置
        return this.getFlushedPosition();
    }

    /**
     * 提交刷盘
     * @param commitLeastPages  最少提交页数
     * @return   提交的offset
     */
    public int commit(final int commitLeastPages) {
        // 如果堆外缓存为null，则无需提交数据到filechannel，所以只需将wrotePosition视为committedPosition返回即可。
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 是否支持提交，判断逻辑和isAbleToFlush方法一致
        if (this.isAbleToCommit(commitLeastPages)) {
            // 增加对该MappedFile的引用次数
            if (this.hold()) {
                // 将堆外内存中的全部脏数据提交到filechannel
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 所有的脏数据被提交到了FileChannel，则归还堆外缓存
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            // 将堆外缓存重置 并 存入内存池availableBuffers的头部
            this.transientStorePool.returnBuffer(writeBuffer);
            // writeBuffer职位null，下次再重新获取
            this.writeBuffer = null;
        }
        // 返回提交位置
        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                // 将writeBuffer中的数据提交到fileChannel中
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 判断是否可以刷盘
     * 会刷盘的三种情况：
     *   情况一：如果文件已满
     *   情况二：如果flushLeastPages大于0 且 脏页数大于等于flushLeastPages
     *   情况三：如果flushLeastPages等于0 且 存在脏数据
     * @param flushLeastPages 至少刷盘的页数
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 获取刷盘位置
        int flush = this.flushedPosition.get();
        // 获取写入位置
        int write = getReadPosition();
        // 如果文件已满，则返回true
        if (this.isFull()) {
            return true;
        }
        /**
         * 如果至少刷盘的页数大于0，则需比较写入位置与刷盘位置的差值
         * 当差值大于等于指定的页数才能刷盘，防止频繁的刷盘
         */
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        // flushLeastPages为0，则只要写入位置大于刷盘位置（即存在脏数据），则就会刷盘
        return write > flush;
    }
    /**
     * 判断是否可以提交
     * 会刷盘的三种情况：
     *   情况一：如果文件已满
     *   情况二：如果commitLeastPages大于0 且 脏页数大于等于commitLeastPages
     *   情况三：如果commitLeastPages等于0 且 存在脏数据
     * @param commitLeastPages 至少刷盘的页数
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    /**
     * MappedFile是否已满（即写入位置等于文件大小）
     *
     * @return
     */
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * MappedFile#selectMappedBuffer(int)
     * 以 pos 为开始位点 到 有效数据为止（数据区间：[pos，文件的截止位点]）， 创建出一个mappedByteBuffer的切片副本，
     * 并封装到SelectMappedBufferResult实例中，供业务访问数据使用
     * @param pos 开始位置（相对偏移量）
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取写入位置（即最大偏移量）
        int readPosition = getReadPosition();
        // 如果指定相对偏移量小于最大偏移量 且 大于等于0，则截取内存（创建一个mappedByteBuffer的切片副本）
        if (pos < readPosition && pos >= 0) {
            // 引用资源（占用资源）：refCount+1
            if (this.hold()) {
                // 从mappedByteBuffer截取一段内存（获取mappedByteBuffer的一个切片副本）
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                // 修改position
                byteBuffer.position(pos);
                // 获取新切片副本的大小
                int size = readPosition - pos;
                // 创建出一个mappedByteBuffer的切片副本（数据区间：[pos，文件的截止位点]）
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                // 根据起始物理索引、新的ByteBuffer、ByteBuffer大小、当前CommitLog对象构建一个SelectMappedBufferResult对象返回
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }
        return null;
    }

    /** MappedFile#cleanup(long)
     * 销毁 mappedByteBuffer
     * @param currentRef 被引用数
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        // 1、是否可用
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }
        // 2、是否已被清理
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }
        // 3、关闭内存映射
        clean(this.mappedByteBuffer);
        // 4、减少当前JVM实例中MappedFile虚拟内存
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        // 5、当前JVM实例中MappedFile对象个数-1
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * MappedFile#destroy(long)
     * 销毁方法
     * @param intervalForcibly  上次关闭时间间隔超过该值则强制删除（第一次拒绝删除（被引用）后，能保留文件的最大时间，CommitLog默认120s、
     *                          IndexFile默认30s、ConsumeQueue默认60s、ConsumeQueueExt默认1s）
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        // 1、关闭（清理）资源（ReferenceResource.shutdown方法）
        this.shutdown(intervalForcibly);
        // 2、是否清理完毕
        if (this.isCleanupOver()) {
            try {
                // 2.1、关闭 fileChannel
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                // 2.2、删除文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * MappedFile#getReadPosition()
     * 具有有效数据的最大位置
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        // 如果writeBuffer为空，则获取当前写文件位置，否则获取提交位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 建立了进程虚拟地址空间映射后，并未分配虚拟内存对应的物理内存，这里进行内存预热
     * @param type   消息刷盘类型，默认 FlushDiskType.ASYNC_FLUSH;
     * @param pages  一页大小，默认4k
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        // 创建一个新的字节缓冲区
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        // 每隔4k大小写入一个0
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            // 每隔4k大小写入一个0
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 如果是同步刷盘，则每次写入都要强制刷盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            /**
             * 调用Thread.sleep(0)当前线程主动放弃CPU资源，立即进入就绪状态
             * 防止因为多次循环导致该线程一直抢占着CPU资源不释放
             */
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 把剩余的数据强制刷新到磁盘中
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);
        // 锁定内存
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    /**
     * 获取切片副本
     * @return
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 锁定内存
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            // mlock调用
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            // madvise调用
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
