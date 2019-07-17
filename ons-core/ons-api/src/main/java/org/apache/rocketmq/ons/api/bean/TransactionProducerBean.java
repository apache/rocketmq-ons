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

package org.apache.rocketmq.ons.api.bean;

import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionChecker;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionExecutor;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;


public class TransactionProducerBean implements TransactionProducer {

    private Properties properties;


    private LocalTransactionChecker localTransactionChecker;

    private TransactionProducer transactionProducer;


    @Override
    public void start() {
        if (null == this.properties) {
            throw new ONSClientException("properties not set");
        }

        this.transactionProducer = ONSFactory.createTransactionProducer(properties, localTransactionChecker);
        this.transactionProducer.start();
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        if (this.transactionProducer != null) {
            this.transactionProducer.updateCredential(credentialProperties);
        }
    }


    @Override
    public void shutdown() {
        if (this.transactionProducer != null) {
            this.transactionProducer.shutdown();
        }
    }

    @Override
    public SendResult send(Message message, LocalTransactionExecutor executer, Object arg) {
        return this.transactionProducer.send(message, executer, arg);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public LocalTransactionChecker getLocalTransactionChecker() {
        return localTransactionChecker;
    }

    public void setLocalTransactionChecker(LocalTransactionChecker localTransactionChecker) {
        this.localTransactionChecker = localTransactionChecker;
    }

    @Override
    public boolean isStarted() {
        return this.transactionProducer.isStarted();
    }

    @Override
    public boolean isClosed() {
        return this.transactionProducer.isClosed();
    }
}
