package com.alibaba.rocketmq.broker.monitor;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.broker.monitor.MsgAccumulationCheckHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.PutMessageStatus;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import io.netty.channel.Channel;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 *
 *
 * @author 张轲
 */
public class MsgAccumulationCheckHookTest {

    @Test
    public void test_sendMessageAfter() throws Exception {
        // init broker
        BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
        boolean initResult = brokerController.initialize();

        brokerController.start();

        // set threshold to a small value to trigger alarm easily
        brokerController.getMonitorManager().setDefaultMsgAccumulationThreshold(3l);

        // create topic
        SendMessageContext context = new SendMessageContext();
        final String topicName = "accumulation-test";
        context.setTopic(topicName);
        context.setQueueId(1);

        brokerController.getTopicConfigManager().createTopicInSendMessageMethod(topicName, MixAll.DEFAULT_TOPIC,null,4,0);

        // send message
        Long cqOffSet = new Long(0);
        for (int count = 0; count < 10; count++) {
            PutMessageResult pmr = putMessage(brokerController, topicName, "I am tester".getBytes());
            assert (pmr.getPutMessageStatus() == PutMessageStatus.PUT_OK);
            cqOffSet = pmr.getAppendMessageResult().getLogicsOffset();
        }

        // register consumer
        SubscriptionData subscriptionData = new SubscriptionData(topicName, "*");
        Set<SubscriptionData> subSets = new HashSet();
        subSets.add(subscriptionData);

        NettyClientConfig config = new NettyClientConfig();
        NettyRemotingClient client = new NettyRemotingClient(config);
        client.start();
        Channel channel = client.getAndCreateChannel("localhost:8888");
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel);

        final String consumerGroup = "zhangkegroupp";
        brokerController.getConsumerManager().registerConsumer(consumerGroup, clientChannelInfo, ConsumeType.CONSUME_PASSIVELY,
                MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, subSets);

        // mock consuming message
        brokerController.getConsumerOffsetManager().commitOffset(topicName + "@" + consumerGroup, consumerGroup, topicName, 0, cqOffSet - 5);

        MsgAccumulationCheckHook hook = new MsgAccumulationCheckHook(brokerController);

        hook.sendMessageAfter(context);

        Thread.sleep(1000 * 600);

        brokerController.shutdown();
    }

    private PutMessageResult putMessage(BrokerController brokerController, String topic, byte[] body) {
        SocketAddress host = getLocal(brokerController);
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(topic);
        inner.setBornHost(host);
        inner.setBornTimestamp(System.currentTimeMillis());
        inner.setStoreHost(host);
        inner.setStoreTimestamp(System.currentTimeMillis());
        inner.setFlag(0);
        inner.setSysFlag(0);
        inner.setQueueId(0);
        inner.setReconsumeTimes(0);
        inner.setBody(body);
        inner.setPropertiesString("");

        PutMessageResult result = brokerController.getMessageStore().putMessage(inner);

        return result;
    }

    private SocketAddress getLocal(BrokerController brokerController) {
        return new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                .getNettyServerConfig().getListenPort());
    }

}