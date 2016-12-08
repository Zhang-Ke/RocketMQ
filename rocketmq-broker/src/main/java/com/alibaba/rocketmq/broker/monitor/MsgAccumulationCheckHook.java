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

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 *
 * @author zhangke
 *
 */
public class MsgAccumulationCheckHook implements SendMessageHook {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MonitorLoggerName);

    private final BrokerController brokerController;

    private final ConcurrentMap<String /* topic */, Long> checkTimeMap = new ConcurrentHashMap();

    private int checkInterval = 1000 * 60;

    private int notifyInterval = 1000 * 60 * 30;

    private final ConcurrentMap<String /* topic */, Long> notifyTimeMap = new ConcurrentHashMap();

    private final SocketAddress host;

    private ExecutorService executor;

    public MsgAccumulationCheckHook(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.host =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                        .getNettyServerConfig().getListenPort());
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, MsgAccumulationCheckHook.class.getName());
            }
        });
    }

    @Override
    public String hookName() {
        return MsgAccumulationCheckHook.class.getSimpleName();
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {

    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        String topic = context.getTopic();

        if (!checkTimeMap.containsKey(topic)) {
            checkTimeMap.putIfAbsent(topic, 0l);
        }

        final long now = System.currentTimeMillis();
        Long lastCheckTime = checkTimeMap.get(topic);
        if (now - lastCheckTime > checkInterval) {
            synchronized (lastCheckTime) {
                if (lastCheckTime != checkTimeMap.get(topic)) {
                    return;
                }
                log.info("Check message accumulation for topic {}", topic);
                executor.submit(new CheckRequest(topic));
                checkTimeMap.put(topic, now);
            }
        }
    }

    private void checkConsumedStats(String topic, int queueId) {
        Set<String> consumerGroups = brokerController.getConsumerOffsetManager().whichGroupByTopic(topic);
        Long maxOffset = brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);

        for (String group : consumerGroups) {
            Long commitOffset = brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
            Long accumulationThreshold = brokerController.getMonitorManager().queryMsgAccumulationThreshold(topic, group);

            final long gap = maxOffset - commitOffset;
            if (gap > accumulationThreshold) {
                if (!notifyTimeMap.containsKey(topic)) {
                    notifyTimeMap.putIfAbsent(topic, 0l);
                }
                Long now = System.currentTimeMillis();
                Long lastNotifyTime = notifyTimeMap.get(topic);
                if (now - lastNotifyTime > notifyInterval) {
                    synchronized (lastNotifyTime) {
                        if (lastNotifyTime != notifyTimeMap.get(topic)) {
                            return;
                        }
                        putNotificationMessage(topic, group, queueId, gap);
                        notifyTimeMap.put(topic, now);
                    }
                }
            }
        }
    }

    private void putNotificationMessage(String topic, String group, int queueId, long gap) {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(MixAll.MSG_ACCUMULATION_NOTIFICATION);
        inner.setBornHost(host);
        inner.setBornTimestamp(System.currentTimeMillis());
        inner.setStoreHost(host);
        inner.setStoreTimestamp(System.currentTimeMillis());
        inner.setFlag(0);
        inner.setSysFlag(0);
        inner.setQueueId(0);
        inner.setReconsumeTimes(0);
        inner.setBody(new NotificationMsg(topic, group, queueId, gap).encode());
        inner.setPropertiesString("");
        String key = UUID.randomUUID().toString();
        inner.setKeys(key);
        log.info("Messages have been accumulated.Send alarm message[topic={},group={},queueId={},gap={}] with key: {}",
                topic, group, queueId, gap, key);
        this.brokerController.getMessageStore().putMessage(inner);
    }


    class CheckRequest implements Runnable {
        private String topic;

        public CheckRequest(String topic) {
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }

        @Override
        public void run() {
            int queueNums = brokerController.getTopicConfigManager().selectTopicConfig(topic).getReadQueueNums();
            for (int i = 0; i < queueNums; i++) {
                checkConsumedStats(topic, i);
            }
        }
    }

    class NotificationMsg extends RemotingSerializable {
        private String topic;
        private String group;
        private int queueId;
        private long gap;

        public NotificationMsg() {
        }

        public NotificationMsg(String topic, String group, int queueId, long gap) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
            this.gap = gap;
        }

        public String getTopic() {
            return topic;
        }

        public String getGroup() {
            return group;
        }

        public int getQueueId() {
            return queueId;
        }

        public long getGap() {
            return gap;
        }
    }
}
