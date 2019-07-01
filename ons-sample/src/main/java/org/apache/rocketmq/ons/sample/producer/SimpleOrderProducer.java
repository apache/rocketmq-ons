/**
 * Copyright (C) 2010-2016 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.rocketmq.ons.sample.producer;

import java.util.Date;
import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.order.OrderProducer;
import org.apache.rocketmq.ons.sample.MQConfig;

public class SimpleOrderProducer {

    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.ORDER_GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
        OrderProducer producer = ONSFactory.createOrderProducer(producerProperties);
        producer.start();
        System.out.println("Producer Started");

        for (int i = 0; i < 10; i++) {
            Message msg = new Message(MQConfig.ORDER_TOPIC, MQConfig.TAG, "MQ send order message test".getBytes());
            String orderId = "biz_" + i % 10;
            msg.setKey(orderId);
            String shardingKey = String.valueOf(orderId);
            try {
                SendResult sendResult = producer.send(msg, shardingKey);
                assert sendResult != null;
                System.out.println(new Date() + " Send mq message success! Topic is: " + MQConfig.ORDER_TOPIC + " msgId is: " + sendResult.getMessageId());
            } catch (ONSClientException e) {
                System.out.println(new Date() + " Send mq message failed! Topic is: " + MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
    }
}
