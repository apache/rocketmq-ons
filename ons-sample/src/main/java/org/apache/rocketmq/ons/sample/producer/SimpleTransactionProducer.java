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
import io.openmessaging.api.SendResult;
import io.openmessaging.api.transaction.LocalTransactionExecuter;
import io.openmessaging.api.transaction.TransactionProducer;
import io.openmessaging.api.transaction.TransactionStatus;
import java.util.Date;
import java.util.Properties;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.sample.MQConfig;

public class SimpleTransactionProducer {

    public static void main(String[] args) {
        MessagingAccessPoint messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876");

        Properties tranProducerProperties = new Properties();
        tranProducerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        tranProducerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        tranProducerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        LocalTransactionCheckerImpl localTransactionChecker = new LocalTransactionCheckerImpl();
        TransactionProducer transactionProducer = messagingAccessPoint.createTransactionProducer(tranProducerProperties, localTransactionChecker);

        /*
         * Alternatively, you can use the ONSFactory to create instance directly.
         * <pre>
         * {@code
         * producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
         * TransactionProducer producer = ONSFactory.createTransactionProducer(tranProducerProperties, localTransactionChecker);
         * }
         * </pre>
         */

        transactionProducer.start();

        Message message = new Message(MQConfig.TOPIC, MQConfig.TAG, "MQ send transaction message test".getBytes());

        for (int i = 0; i < 10; i++) {
            try {
                SendResult sendResult = transactionProducer.send(message, new LocalTransactionExecuter() {
                    @Override
                    public TransactionStatus execute(Message msg, Object arg) {
                        System.out.printf("Execute local transaction and return TransactionStatus. %s %n", msg);
                        return TransactionStatus.Unknow;
                    }
                }, null);
                assert sendResult != null;
            } catch (ONSClientException e) {
                System.out.printf(new Date() + " Send mq message failed! Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
        transactionProducer.shutdown();
        System.out.printf("Send transaction message success. %n");
    }
}