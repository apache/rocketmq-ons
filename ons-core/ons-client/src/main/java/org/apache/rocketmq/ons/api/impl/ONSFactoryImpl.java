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

package org.apache.rocketmq.ons.api.impl;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.Properties;
import org.apache.rocketmq.ons.api.Constants;
import org.apache.rocketmq.ons.api.Consumer;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactoryAPI;
import org.apache.rocketmq.ons.api.Producer;
import org.apache.rocketmq.ons.api.batch.BatchConsumer;
import org.apache.rocketmq.ons.api.impl.rocketmq.BatchConsumerImpl;
import org.apache.rocketmq.ons.api.impl.rocketmq.ConsumerImpl;
import org.apache.rocketmq.ons.api.impl.rocketmq.ONSUtil;
import org.apache.rocketmq.ons.api.impl.rocketmq.OrderConsumerImpl;
import org.apache.rocketmq.ons.api.impl.rocketmq.OrderProducerImpl;
import org.apache.rocketmq.ons.api.impl.rocketmq.ProducerImpl;
import org.apache.rocketmq.ons.api.impl.rocketmq.TransactionProducerImpl;
import org.apache.rocketmq.ons.api.order.OrderConsumer;
import org.apache.rocketmq.ons.api.order.OrderProducer;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionChecker;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;
import org.apache.rocketmq.ons.api.transaction.TransactionStatus;

public class ONSFactoryImpl implements ONSFactoryAPI {
    @Override
    public Producer createProducer(final Properties properties) {
        return new ProducerImpl(ONSUtil.extractProperties(properties));
    }


    @Override
    public Consumer createConsumer(final Properties properties) {
        return new ConsumerImpl(ONSUtil.extractProperties(properties));
    }

    @Override
    public BatchConsumer createBatchConsumer(final Properties properties) {
        return new BatchConsumerImpl(ONSUtil.extractProperties(properties));
    }

    @Override
    public OrderProducer createOrderProducer(final Properties properties) {
        return new OrderProducerImpl(ONSUtil.extractProperties(properties));
    }


    @Override
    public OrderConsumer createOrderedConsumer(final Properties properties) {
        return new OrderConsumerImpl(ONSUtil.extractProperties(properties));
    }

    @Override
    public TransactionProducer createTransactionProducer(Properties properties,
                                                         final LocalTransactionChecker checker) {
        return new TransactionProducerImpl(ONSUtil.extractProperties(properties), new TransactionCheckListener() {
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                String msgId = msg.getProperty(Constants.TRANSACTION_ID);
                Message message = ONSUtil.msgConvert(msg);
                message.setMsgID(msgId);
                TransactionStatus check = checker.check(message);
                if (TransactionStatus.CommitTransaction == check) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (TransactionStatus.RollbackTransaction == check) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.UNKNOW;
            }
        });
    }
}
