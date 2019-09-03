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

package org.apache.rocketmq.ons.api.impl.rocketmq;

import io.openmessaging.Consumer;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.Producer;
import io.openmessaging.SendResult;
import io.openmessaging.exception.OMSRuntimeException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.ons.api.impl.authority.SessionCredentials;
import org.apache.rocketmq.ons.api.impl.constant.PropertyKeyConst;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ONSClientTokenUpdateTest {

    private static final String TOPIC = "STS_TOPIC_TEST_MOLING";

    private MessagingAccessPoint messagingAccessPoint;

    @Before
    public void init() {
        messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://alice@rocketmq.apache.org/us-east");
    }


    @Ignore
    public void testSend() throws InterruptedException {

        List<List<String>> credentials = new ArrayList<List<String>>() {
            {
                add(new ArrayList<String>() {
                    {
                        add("ak");
                        add("sk");
                        add("token");
                    }
                });

                add(new ArrayList<String>() {
                    {
                        add("ak");
                        add("sk");
                        add("token");
                    }
                });

            }
        };

        Producer producer = messagingAccessPoint.createProducer(buildProps(
            "ak",
            "sk",
            "token",
            ONSChannel.ALIYUN.name()
        ));
        producer.start();

        for (int i = 0; i < 100; i++) {
            List<String> credential = credentials.get(i % credentials.size());
            producer.updateCredential(buildProps(credential.get(0), credential.get(1), credential.get(2), ONSChannel.ALIYUN.name()));
            try {
                Message msg = new Message(TOPIC, "tag", "key" + i, ("content." + i).getBytes());
                SendResult result = producer.send(msg);
                System.out.println(i + " use ak " + credential.get(0) + " send " + result.getMessageId());
            } catch (Exception e) {
                System.out.println(i + " use ak " + credential.get(0) + " send failed.");
            }
        }

        Thread.sleep(10 * 1000L);
        producer.shutdown();
    }

    @Test
    public void test_ConsumerImpl() throws NoSuchFieldException, IllegalAccessException {
        Consumer consumer = messagingAccessPoint.createConsumer(buildProps("ak", "sk", "token", ONSChannel.ALIYUN.name()));
        ONSConsumerAbstract subImpl = (ONSConsumerAbstract) consumer;
        consumer.start();

        Assert.assertTrue(subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        ClientRPCHook clientRPCHook = (ClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = ClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        SessionCredentials credentials = (SessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.ALIYUN, credentials.getOnsChannel());

        consumer.updateCredential(buildProps("nak", "nsk", "ntoken", ONSChannel.CLOUD.name()));

        Assert.assertEquals("nak", credentials.getAccessKey());
        Assert.assertEquals("nsk", credentials.getSecretKey());
        Assert.assertEquals("ntoken", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.CLOUD, credentials.getOnsChannel());
    }

    @Test
    public void test_ProducerImpl() throws NoSuchFieldException, IllegalAccessException {
        Producer producer = messagingAccessPoint.createProducer(buildProps("ak", "sk", "token", ONSChannel.ALIYUN.name()));
        ProducerImpl subImpl = (ProducerImpl) producer;
        producer.start();

        Assert.assertTrue(subImpl.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        ClientRPCHook clientRPCHook = (ClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = ClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        SessionCredentials credentials = (SessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.ALIYUN, credentials.getOnsChannel());

        producer.updateCredential(buildProps("nak", "nsk", "ntoken", ONSChannel.CLOUD.name()));

        Assert.assertEquals("nak", credentials.getAccessKey());
        Assert.assertEquals("nsk", credentials.getSecretKey());
        Assert.assertEquals("ntoken", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.CLOUD, credentials.getOnsChannel());
    }

    @Test
    public void test_ConsumerImpl_updateNull() throws NoSuchFieldException, IllegalAccessException {
        Consumer consumer = messagingAccessPoint.createConsumer(buildProps("ak", "sk", "token", ONSChannel.ALIYUN.name()));
        ONSConsumerAbstract subImpl = (ONSConsumerAbstract) consumer;
        consumer.start();

        Assert.assertTrue(subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        ClientRPCHook clientRPCHook = (ClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = ClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        SessionCredentials credentials = (SessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.ALIYUN, credentials.getOnsChannel());

        try {
            consumer.updateCredential(buildProps("nak", "", "ntoken", ONSChannel.CLOUD.name()));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof OMSRuntimeException);
        }

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals(ONSChannel.ALIYUN, credentials.getOnsChannel());
    }

    private static Properties buildProps(String ak, String sk, String token, String channel) {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.ConsumerId, "CID_STS_TEST_MOLING");
        properties.put(PropertyKeyConst.ProducerId, "PID_STS_TEST_MOLING");
        properties.put(PropertyKeyConst.AccessKey, ak);
        properties.put(PropertyKeyConst.SecretKey, sk);
        properties.put(PropertyKeyConst.SecurityToken, token);
        properties.put(PropertyKeyConst.OnsChannel, channel);

        return properties;
    }

}
