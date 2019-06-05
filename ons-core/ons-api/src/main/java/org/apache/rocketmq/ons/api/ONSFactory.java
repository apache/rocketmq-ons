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
package org.apache.rocketmq.ons.api;

import org.apache.rocketmq.ons.api.batch.BatchConsumer;
import org.apache.rocketmq.ons.api.order.OrderConsumer;
import org.apache.rocketmq.ons.api.order.OrderProducer;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionChecker;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;
import java.util.Properties;


public class ONSFactory {


    private static ONSFactoryAPI onsFactory = null;

    static {
        try {

            Class<?> factoryClass =
                    ONSFactory.class.getClassLoader().loadClass(
                            "org.apache.rocketmq.ons.api.impl.ONSFactoryNotifyAndMetaQImpl");
            onsFactory = (ONSFactoryAPI) factoryClass.newInstance();
        } catch (Throwable e) {
            try {
                Class<?> factoryClass =
                        ONSFactory.class.getClassLoader().loadClass(
                                "org.apache.rocketmq.ons.api.impl.ONSFactoryImpl");
                onsFactory = (ONSFactoryAPI) factoryClass.newInstance();
            } catch (Throwable e1) {
                e.printStackTrace();
                e1.printStackTrace();
            }
        }
    }



    public static Producer createProducer(final Properties properties) {
        return onsFactory.createProducer(properties);
    }



    public static OrderProducer createOrderProducer(final Properties properties) {
        return onsFactory.createOrderProducer(properties);
    }



    public static TransactionProducer createTransactionProducer(final Properties properties,
                                                                final LocalTransactionChecker checker) {
        return onsFactory.createTransactionProducer(properties, checker);
    }



    public static Consumer createConsumer(final Properties properties) {
        return onsFactory.createConsumer(properties);
    }


    public static BatchConsumer createBatchConsumer(final Properties properties) {
        return onsFactory.createBatchConsumer(properties);
    }



    public static OrderConsumer createOrderedConsumer(final Properties properties) {
        return onsFactory.createOrderedConsumer(properties);
    }

}
