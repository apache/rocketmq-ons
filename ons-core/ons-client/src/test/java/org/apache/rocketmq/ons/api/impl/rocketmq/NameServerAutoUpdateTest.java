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

import io.openmessaging.Consumer;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.Producer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.order.OrderProducer;
import java.util.Properties;
import org.apache.rocketmq.ons.api.impl.constant.PropertyKeyConst;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class NameServerAutoUpdateTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MessagingAccessPoint messagingAccessPoint;

    @Before
    public void init() {
        messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://alice@rocketmq.apache.org/us-east");
    }

    @org.junit.Test
    public void testNamesrv_setNsAddr() {
        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.NAMESRV_ADDR, "xxx-whatever");
        Consumer consumer = messagingAccessPoint.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_setOnsAddr_invalid() {
        expectedException.expect(OMSRuntimeException.class);
        expectedException.expectMessage("onsAddr " + "xxx");

        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.ONSAddr, "xxx");
        Consumer consumer = messagingAccessPoint.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_setOnsAddr_valid() throws InterruptedException {
        Properties prop = buildProps();
        prop.setProperty(PropertyKeyConst.ONSAddr, "http://xxxx/rocketmq/nsaddr4client-internet");
        Consumer consumer = messagingAccessPoint.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default() throws InterruptedException {
        Properties prop = buildProps();
        Consumer consumer = messagingAccessPoint.createConsumer(prop);
        consumer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default_Producer() throws InterruptedException {
        Properties prop = buildProps();
        Producer producer = messagingAccessPoint.createProducer(prop);
        producer.start();
    }

    @org.junit.Test
    public void testNamesrv_notSetOnsAddr_useInternet_default_OrderProcucer() throws InterruptedException {
        Properties prop = buildProps();
        OrderProducer producer = messagingAccessPoint.createOrderProducer(prop);
        producer.start();
    }

    private static Properties buildProps() {
        Properties properties = new Properties();

        properties.put(PropertyKeyConst.GROUP_ID, "group");
        properties.put(PropertyKeyConst.AccessKey, "XXX");
        properties.put(PropertyKeyConst.SecretKey, "XXX");
        return properties;
    }

}
