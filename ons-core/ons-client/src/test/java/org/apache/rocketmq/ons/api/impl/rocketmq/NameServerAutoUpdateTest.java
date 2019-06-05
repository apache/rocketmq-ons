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

import org.apache.rocketmq.ons.api.Consumer;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.Producer;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.order.OrderProducer;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Properties;

public class NameServerAutoUpdateTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @org.junit.Test
    public void testNamesrv_setNsAddr() {
        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.NAMESRV_ADDR, "xxx-whatever");
        Consumer consumer = ONSFactory.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_setOnsAddr_invalid() {
        expectedException.expect(ONSClientException.class);
        expectedException.expectMessage("onsAddr " + "xxx");

        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.ONSAddr, "xxx");
        Consumer consumer = ONSFactory.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_setOnsAddr_valid() throws InterruptedException {
        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.ONSAddr, "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet");
        Consumer consumer = ONSFactory.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default() throws InterruptedException {
        Properties prop = buildProps();
        Consumer consumer = ONSFactory.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default_Producer() throws InterruptedException {
        Properties prop = buildProps();
        Producer producer = ONSFactory.createProducer(prop);
        producer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default_OrderProcucer() throws InterruptedException {
        Properties prop = buildProps();
        OrderProducer producer = ONSFactory.createOrderProducer(prop);
        producer.start();
    }

    private static Properties buildProps() {
        Properties properties = new Properties();

        properties.put(PropertyKeyConst.ConsumerId, "metaq-consumer-01_SELF");
        // 鉴权用 AccessKey，在阿里云服务器管理控制台创建
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        // 鉴权用 SecretKey，在阿里云服务器管理控制台创建
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        return properties;
    }

}
