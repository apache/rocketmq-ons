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

import java.util.Date;
import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionExecutor;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;
import org.apache.rocketmq.ons.api.transaction.TransactionStatus;
import org.apache.rocketmq.ons.sample.MQConfig;

public class SimpleTransactionProducer {

    public static void main(String[] args) {
        Properties tranProducerProperties = new Properties();
        tranProducerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        tranProducerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        tranProducerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        tranProducerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
        LocalTransactionCheckerImpl localTransactionChecker = new LocalTransactionCheckerImpl();
        TransactionProducer transactionProducer = ONSFactory.createTransactionProducer(tranProducerProperties, localTransactionChecker);
        transactionProducer.start();

        Message message = new Message(MQConfig.TOPIC, MQConfig.TAG, "MQ send transaction message test".getBytes());

        for (int i = 0; i < 10; i++) {
            try {
                SendResult sendResult = transactionProducer.send(message, new LocalTransactionExecutor() {
                    @Override
                    public TransactionStatus execute(Message msg, Object arg) {
                        System.out.printf("Execute local transaction and return TransactionStatus. %n");
                        return TransactionStatus.CommitTransaction;
                    }

                }, null);
                assert sendResult != null;
            } catch (ONSClientException e) {
                System.out.printf(new Date() + " Send mq message failed! Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }

        System.out.printf("Send transaction message success. %n");
    }
}