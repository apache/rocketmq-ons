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
package org.apache.rocketmq.ons.sample.producer;

import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.SendResult;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.order.OrderProducer;
import java.util.Properties;
import org.apache.rocketmq.ons.api.impl.constant.PropertyKeyConst;
import org.apache.rocketmq.ons.sample.MQConfig;

public class SimpleOrderProducer {

    public static void main(String[] args) {
        MessagingAccessPoint messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://alice@rocketmq.apache.org/us-east");

        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.ORDER_GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
        OrderProducer producer = messagingAccessPoint.createOrderProducer(producerProperties);
        producer.start();
        System.out.printf("Producer Started. %n");

        for (int i = 0; i < 10; i++) {
            Message msg = new Message(MQConfig.ORDER_TOPIC, MQConfig.TAG, "MQ send order message test".getBytes());
            String orderId = "biz_" + i % 10;
            msg.setKey(orderId);
            String shardingKey = String.valueOf(orderId);
            try {
                SendResult sendResult = producer.send(msg, shardingKey);
                assert sendResult != null;
                System.out.printf("Send mq timer message success! Topic is: %s msgId is: %s%n", MQConfig.TOPIC, sendResult.getMessageId());
            } catch (OMSRuntimeException e) {
                System.out.printf("Send mq message failed. Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
    }
}
