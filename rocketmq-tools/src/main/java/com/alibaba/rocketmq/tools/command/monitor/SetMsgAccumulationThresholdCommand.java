/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.monitor;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Set;


/**
 * @author zhangke
 */
public class SetMsgAccumulationThresholdCommand implements SubCommand {

    @Override
    public String commandName() {
        return "setMsgAccumulationThreshold";
    }


    @Override
    public String commandDesc() {
        return "Set message accumulation threshold used for monitor";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "threshold", true, "threshold of message accumulation");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "set message accumulation to which cluster");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue("t").trim();
            String consumerGroup = commandLine.getOptionValue("g").trim();
            long threshold = Long.parseLong(commandLine.getOptionValue("v").trim());
            String clusterName = commandLine.getOptionValue("c").trim();

            Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            for (String addr : masterSet) {
                defaultMQAdminExt.setMsgAccumulationThreshold(addr,topic, consumerGroup, threshold);
                System.out.printf("set message accumulation threshold to %s success.%n", addr);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
