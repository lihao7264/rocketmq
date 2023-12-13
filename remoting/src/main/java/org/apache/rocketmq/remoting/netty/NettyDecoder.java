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
package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Netty解码器
 * 用于将 byte[] 解码为 RocketMQ的通信层协议传输对象RemotingCommand
 * 标注了Sharable注解，表示公用Handler，无需为每个socket连接创建相应的handler
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 每帧最大长度：16777216（16MB）
     */
    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        /**
         * 参数一：最大帧长度 16777216（16MB）
         * 参数二：长度域开始偏移量
         * 参数三：长度域长度
         * 参数四：长度调整参数
         * 参数五：接收到的数据包去除前initialBytesToStrip位
         * 整体的配置生效后：传输数据格式为 前4个字节为总数据长度，根据总数据长度截取后续对应的数据部分
         */
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            // 根据指定格式获取一个数据帧
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            // 返回 RemotingCommand 对象
            return RemotingCommand.decode(frame);
        } catch (Exception e) {
            // 打印日志相关
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            // 关闭通道
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            // 使用完数据帧后，需释放buffer；否则会造成内存泄漏
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
