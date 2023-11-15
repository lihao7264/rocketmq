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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

public class NamesrvStartup {

    private static InternalLogger log;
    // 配置文件中的配置信息
    private static Properties properties = null;
    // mqnamesrv命令行
    private static CommandLine commandLine = null;

    /**
     * NameServer的启动入口
     * @param args
     */
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        try {
            /*
             * 1、创建NamesrvController
             */
            NamesrvController controller = createNamesrvController(args);
            /*
             * 2、启动NamesrvController
             */
            start(controller);
            /*
             * 启动成功打印日志
             */
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 创建NamesrvController（NameServer的一个中央控制器类）
     * 1.解析命令行.
     * 2.加载NameServer配置和NettyServer各种配置（解析命令行中-c指定的配置文件）并保存起来.
     * 3.创建一个NamesrvController。
     * @param args
     * @return
     * @throws IOException
     * @throws JoranException
     */
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        /**
         * 设置RocketMQ的版本信息
         * 属性名：rocketmq.remoting.version
         */
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();
        /**
         * jar包启动时，构建命令行操作的指令，使用main方法启动可忽略
         */
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // mqnamesrv命令文件
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }
        // 创建NameServer的配置类（包含NameServer的配置）
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // NettyServer的配置类
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // Netty服务的监听端口设置为9876
        nettyServerConfig.setListenPort(9876);
        /**
         * 判断命令行中是否包含字符'c'（即是否包含通过命令行指定配置文件的命令）
         * 比如：启动Broker时，添加的 -c /Volumes/Samsung/Idea/rocketmq/config/conf/broker.conf命令
         */
        if (commandLine.hasOption('c')) {
            /**
             * 解析配置文件 且 存入NamesrvConfig、NettyServerConfig中
             */
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        /**
         * 判断命令行中是否包含字符'p'，如果存在则打印配置信息并结束jvm运行，没有的话则不用管
         */
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }
        // 把命令行的配置解析到namesrvConfig
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        // 如果不存在ROCKETMQ_HOME的配置，则打印异常并退出程序（最开始启动NameServer是抛出异常的位置）
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        /**
         * 一系列日志的配置
         **/
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
        // 打印nameServer 服务器配置类和 Netty 服务器配置类的配置信息
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        /**
         * 根据NamesrvConfig和NettyServerConfig创建NamesrvController
         */
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        // 将所有的-c的外部配置信息保存到NamesrvController中的Configuration对象属性的allConfigs属性中
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    // 启动
    public static NamesrvController start(final NamesrvController controller) throws Exception {
        // 不能为null
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        /*
         * 1 初始化NettyServer
         *   创建Netty远程服务，初始化Netty线程池，注册请求处理器，配置定时任务，用于扫描并移除不活跃的Broker等操作。
         */
        boolean initResult = controller.initialize();
        // 初始化失败则退出程序
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        /*
         * 2 添加关闭钩子方法，在NameServer关闭之前执行，进行一些内存清理、对象销毁等操作
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));
        /*
         * 3 启动NettyServer 并 进行监听
         */
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
