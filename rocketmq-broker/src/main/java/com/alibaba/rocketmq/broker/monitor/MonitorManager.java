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
package com.alibaba.rocketmq.broker.monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.admin.MsgAccumulationThreshold;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.body.MsgAccumulationThresholdWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 *
 * @author zhangke
 */
public class MonitorManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MonitorLoggerName);

    private static final String MONITOR_DIRECTORY_PATH = System.getProperty("user.home") + File.separator + "monitor";

    private static final String MESSAGE_ACCUMULATION_CONFIG_FILE_PATH = MONITOR_DIRECTORY_PATH + File.separator + "message-accumulation.json";

    private static final String KEY_SEPARATOR = "@";

    private ConcurrentMap<String /* topic@group */, Long> thresholdTable = new ConcurrentHashMap();

    private Long defaultMsgAccumulationThreshold = Long.MAX_VALUE;

    private File configFile;

    private transient BrokerController brokerController;

    public MonitorManager() {
        init();
    }

    public MonitorManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        init();
    }

    private void init() {
        File dir = new File(MONITOR_DIRECTORY_PATH);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        configFile = new File(MESSAGE_ACCUMULATION_CONFIG_FILE_PATH);
        if (!configFile.exists()) {
            try {
                configFile.createNewFile();
            } catch (IOException e) {
                log.error("fail to create {}", configFile.getAbsolutePath());
            }
        }

        String content = MixAll.file2String(MESSAGE_ACCUMULATION_CONFIG_FILE_PATH);
        if (content != null && content.length() != 0) {
            List<MsgAccumulationThreshold> dataList = JSON.parseArray(content, MsgAccumulationThreshold.class);
            for (MsgAccumulationThreshold data : dataList) {
                thresholdTable.put(getKey(data.getTopic(), data.getConsumerGroup()), data.getThreshold());
            }
        }
    }

    private String getKey(String topic, String consumerGroup) {
        return topic + KEY_SEPARATOR + consumerGroup;
    }

    public synchronized void setMsgAccumulationThreshold(String topic, String consumerGroup, final long msgAccumulationThreshold) throws IOException {
        String content = MixAll.file2String(MESSAGE_ACCUMULATION_CONFIG_FILE_PATH);
        List<MsgAccumulationThreshold> dataList = JSON.parseArray(content, MsgAccumulationThreshold.class);

        boolean updated = false;
        for (MsgAccumulationThreshold data : dataList) {
            if (data.getConsumerGroup().equalsIgnoreCase(consumerGroup) && data.getTopic().equalsIgnoreCase(topic)) {
                data.setThreshold(msgAccumulationThreshold);
                updated = true;
            }
        }
        if (!updated) {
            dataList.add(new MsgAccumulationThreshold(topic, consumerGroup, msgAccumulationThreshold));
        }

        MixAll.string2File(JSON.toJSONString(dataList), MESSAGE_ACCUMULATION_CONFIG_FILE_PATH);

        thresholdTable.put(getKey(topic, consumerGroup), msgAccumulationThreshold);
    }

    public Long queryMsgAccumulationThreshold(String topic, String consumerGroup) {
        Long threshold = thresholdTable.get(getKey(topic, consumerGroup));
        if (threshold == null) {
            return defaultMsgAccumulationThreshold;
        }

        return threshold;
    }

    public void setDefaultMsgAccumulationThreshold(Long defaultMsgAccumulationThreshold) {
        this.defaultMsgAccumulationThreshold = defaultMsgAccumulationThreshold;
    }

    public MsgAccumulationThresholdWrapper queryAllMsgAccumulationThresholds() {
        MsgAccumulationThresholdWrapper wrapper = new MsgAccumulationThresholdWrapper();

        for (Iterator<Map.Entry<String, Long>> iter = thresholdTable.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, Long> next = iter.next();
            String key = next.getKey();
            Long value = next.getValue();

            MsgAccumulationThreshold threshold = new MsgAccumulationThreshold();
            String[] arr = key.split(KEY_SEPARATOR);
            threshold.setTopic(arr[0]);
            threshold.setConsumerGroup(arr[1]);
            threshold.setThreshold(value);

            wrapper.addThreshold(threshold);
        }

        return wrapper;
    }
}
