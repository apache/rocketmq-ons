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

import io.openmessaging.api.Consumer;
import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendResult;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.impl.authority.exception.OnsSessionCredentials;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ONSClientTokenUpdateTest {

    private static final String TOPIC = "TOPIC_TEST";

    private MessagingAccessPoint messagingAccessPoint;

    @Before
    public void init() {
        messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876");
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
            "ALIYUN"
        ));
        producer.start();

        for (int i = 0; i < 100; i++) {
            List<String> credential = credentials.get(i % credentials.size());
            producer.updateCredential(buildProps(credential.get(0), credential.get(1), credential.get(2), "ALIYUN"));
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
        Consumer consumer = messagingAccessPoint.createConsumer(buildProps("ak", "sk", "token", "ALIYUN"));
        ONSConsumerAbstract subImpl = (ONSConsumerAbstract) consumer;
        consumer.start();

        Assert.assertTrue(subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        AclClientRPCHook clientRPCHook = (AclClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = AclClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        OnsSessionCredentials credentials = (OnsSessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());

        consumer.updateCredential(buildProps("nak", "nsk", "ntoken", "ALIYUN"));

        Assert.assertEquals("nak", credentials.getAccessKey());
        Assert.assertEquals("nsk", credentials.getSecretKey());
        Assert.assertEquals("ntoken", credentials.getSecurityToken());
        Assert.assertEquals("ALIYUN", credentials.getOnsChannel());

    }

    @Test
    public void test_ProducerImpl() throws NoSuchFieldException, IllegalAccessException {
        Producer producer = messagingAccessPoint.createProducer(buildProps("ak", "sk", "token", "testChannel"));
        ProducerImpl subImpl = (ProducerImpl) producer;
        producer.start();

        Assert.assertTrue(subImpl.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        AclClientRPCHook clientRPCHook = (AclClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = AclClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        OnsSessionCredentials credentials = (OnsSessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());

        producer.updateCredential(buildProps("nak", "nsk", "ntoken", "testChannel2"));

        Assert.assertEquals("nak", credentials.getAccessKey());
        Assert.assertEquals("nsk", credentials.getSecretKey());
        Assert.assertEquals("ntoken", credentials.getSecurityToken());
        Assert.assertEquals("testChannel2", credentials.getOnsChannel());

    }

    @Test
    public void test_ConsumerImpl_updateNull() throws NoSuchFieldException, IllegalAccessException {
        Consumer consumer = messagingAccessPoint.createConsumer(buildProps("ak", "sk", "token", "testChannel"));
        ONSConsumerAbstract subImpl = (ONSConsumerAbstract) consumer;
        consumer.start();

        Assert.assertTrue(subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient() instanceof NettyRemotingClient);
        NettyRemotingClient remotingClient =
            (NettyRemotingClient) subImpl.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().getRemotingClient();
        Assert.assertTrue(remotingClient.getRPCHooks() instanceof List);
        AclClientRPCHook clientRPCHook = (AclClientRPCHook) remotingClient.getRPCHooks().get(0);
        Field field = AclClientRPCHook.class.getDeclaredField("sessionCredentials");
        field.setAccessible(true);
        OnsSessionCredentials credentials = (OnsSessionCredentials) field.get(clientRPCHook);

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals("testChannel", credentials.getOnsChannel());

        try {
            consumer.updateCredential(buildProps("nak", "", "ntoken", "testChannel2"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ONSClientException);
        }

        Assert.assertEquals("ak", credentials.getAccessKey());
        Assert.assertEquals("sk", credentials.getSecretKey());
        Assert.assertEquals("token", credentials.getSecurityToken());
        Assert.assertEquals("testChannel2", credentials.getOnsChannel());
    }

    private static Properties buildProps(String ak, String sk, String token, String channel) {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.GROUP_ID, "CID_STS_TEST_MOLING");
        properties.put(PropertyKeyConst.AccessKey, ak);
        properties.put(PropertyKeyConst.SecretKey, sk);
        properties.put(PropertyKeyConst.SecurityToken, token);
        properties.put(PropertyKeyConst.OnsChannel, channel);

        return properties;
    }

}
