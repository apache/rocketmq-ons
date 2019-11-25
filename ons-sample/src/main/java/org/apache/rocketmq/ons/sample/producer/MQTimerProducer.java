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


import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.Properties;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.sample.MQConfig;

public class MQTimerProducer {
    public static void main(String[] args) {
        MessagingAccessPoint messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876");

        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        Producer producer = messagingAccessPoint.createProducer(producerProperties);

        /*
         * Alternatively, you can use the ONSFactory to create instance directly.
         * <pre>
         * {@code
         * producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
         * OrderProducer producer = ONSFactory.createOrderProducer(producerProperties);
         * }
         * </pre>
         */

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
            } catch (OMSRuntimeException e) {
                System.out.printf("Send mq message failed. Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
