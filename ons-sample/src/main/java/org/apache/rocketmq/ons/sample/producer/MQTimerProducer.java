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

import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.Producer;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.sample.MQConfig;

public class MQTimerProducer {
    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
        Producer producer = ONSFactory.createProducer(producerProperties);
        producer.start();
        System.out.printf("Producer Started. %n");

        for (int i = 0; i < 10; i++) {
            Message message = new Message(MQConfig.TOPIC, MQConfig.TAG, "MQ send timer message test".getBytes());
            long delayTime = 3000;
            message.setStartDeliverTime(System.currentTimeMillis() + delayTime);
            try {
                SendResult sendResult = producer.send(message);
                assert sendResult != null;
                System.out.printf("Send mq timer message success! Topic is: %s msgId is: %s%n", MQConfig.TOPIC, sendResult.getMessageId());
            } catch (ONSClientException e) {
                System.out.printf("Send mq message failed. Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
    }
}
