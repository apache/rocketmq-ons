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

import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionExecuter;
import org.apache.rocketmq.ons.api.transaction.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LocalTransactionExecuter} decorator, used to handle uncaught exception of {@link
 * LocalTransactionExecuter#execute(Message, Object)}. Since uncaught exception from {@code execute} will cause issue
 * and it's not very clear to find it out from log, in case of forgetting to catch exception for {@link
 * LocalTransactionExecuter} implementation, a decorator could solve it simply.
 * <p>
 * Technical detail:
 * <br>When {@link LocalTransactionExecuter#execute(Message, Object)} throw exception, underlying {@link
 * org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendMessageInTransaction} won't throw exception and
 * return instance of {@link org.apache.rocketmq.client.producer.TransactionSendResult} with {@link
 * org.apache.rocketmq.client.producer.LocalTransactionState#UNKNOW}. Then {@link org.apache.rocketmq.ons.api.transaction.TransactionProducer#send(Message,
 * LocalTransactionExecuter, Object)} will return instance of {@link org.apache.rocketmq.ons.api.SendResult}, as
 * invoker, it is impossible to know whether {@code send} is success or not. So it's not recommend to throw exception
 * from {@code send}, following the comment of {@link org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently#consumeMessage(java.util.List,
 * org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext)}.
 */
public class LocalTransactionExecuterDecorator implements LocalTransactionExecuter {

    //private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTransactionExecuterDecorator.class);

    private final LocalTransactionExecuter localTransactionExecuter;

    public LocalTransactionExecuterDecorator(
        LocalTransactionExecuter localTransactionExecuter) {
        this.localTransactionExecuter = localTransactionExecuter;
    }

    @Override public TransactionStatus execute(Message msg, Object arg) {
        try {
            return localTransactionExecuter.execute(msg, arg);
        } catch (Throwable e) {
            LOGGER.error("localTransactionExecuter.execute failed", e);
            return TransactionStatus.RollbackTransaction;
        }
    }

}
