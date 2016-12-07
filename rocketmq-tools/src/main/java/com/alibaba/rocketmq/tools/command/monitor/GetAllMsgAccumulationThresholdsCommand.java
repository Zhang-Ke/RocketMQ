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

import com.alibaba.rocketmq.common.admin.MsgAccumulationThreshold;
import com.alibaba.rocketmq.common.protocol.body.MsgAccumulationThresholdWrapper;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Set;


/**
 * @author zhangke
 */
public class GetAllMsgAccumulationThresholdsCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getAllMsgAccumulationThresholds";
    }


    @Override
    public String commandDesc() {
        return "Get all message accumulation thresholds";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "which cluster to get");
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
            String clusterName = commandLine.getOptionValue("c").trim();

            Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            MsgAccumulationThresholdWrapper wrapper = defaultMQAdminExt.getAllMsgAccumulationThresholds(masterSet.iterator().next());
            List<MsgAccumulationThreshold> thresholds = wrapper.getThresholds();
            System.out.printf("%-50s  %-30s  %-15s  %n",//
                    "topic",//
                    "consumerGroup",//
                    "threshold");
            if (!thresholds.isEmpty()) {
                for (MsgAccumulationThreshold t : thresholds) {
                    System.out.printf("%-50s  %-30s  %-15d  %n",//
                            t.getTopic(),//
                            t.getConsumerGroup(),//
                            t.getThreshold());
                }
            } else {
                System.out.println("Haven't set any threshold of message accumulation yet ");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
