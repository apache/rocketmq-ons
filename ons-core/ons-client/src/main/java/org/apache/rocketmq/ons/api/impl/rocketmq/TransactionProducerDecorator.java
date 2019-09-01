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

import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionExecuter;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;

/**
 * A {@link TransactionProducer} decorator. Enable {@link LocalTransactionExecuterDecorator}.
 */
public class TransactionProducerDecorator implements TransactionProducer {

    private final TransactionProducer transactionProducer;

    public TransactionProducerDecorator(TransactionProducer transactionProducer) {
        this.transactionProducer = transactionProducer;
    }

    @Override public boolean isStarted() {
        return transactionProducer.isStarted();
    }

    @Override public boolean isClosed() {
        return transactionProducer.isClosed();
    }

    @Override public void start() {
        transactionProducer.start();
    }

    @Override public void updateCredential(Properties credentialProperties) {
        transactionProducer.updateCredential(credentialProperties);
    }

    @Override public void shutdown() {
        transactionProducer.shutdown();
    }

    @Override public SendResult send(Message message, LocalTransactionExecuter executer, Object arg) {
        return transactionProducer.send(message, new LocalTransactionExecuterDecorator(executer), arg);
    }

}
