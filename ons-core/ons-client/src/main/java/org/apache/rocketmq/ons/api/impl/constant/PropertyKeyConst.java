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
package org.apache.rocketmq.ons.api.impl.constant;

public class PropertyKeyConst {

    public static final String MessageModel = "MessageModel";

    /**
     * Deprecated, replaced with GROUP_ID
     */
    @Deprecated
    public static final String ProducerId = "ProducerId";

    /**
     * Deprecated, replaced with GROUP_ID
     */
    @Deprecated
    public static final String ConsumerId = "ConsumerId";

    public static final String GROUP_ID = "GROUP_ID";

    public static final String AccessKey = "AccessKey";

    public static final String SecretKey = "SecretKey";

    public static final String SecurityToken = "SecurityToken";

    public static final String SendMsgTimeoutMillis = "SendMsgTimeoutMillis";

    public static final String ONSAddr = "ONSAddr";

    public static final String NAMESRV_ADDR = "NAMESRV_ADDR";

    public static final String ConsumeThreadNums = "ConsumeThreadNums";

    public static final String OnsChannel = "OnsChannel";

    public static final String MQType = "MQType";

    public static final String isVipChannelEnabled = "isVipChannelEnabled";

    public static final String SuspendTimeMillis = "suspendTimeMillis";

    public static final String MaxReconsumeTimes = "maxReconsumeTimes";

    public static final String ConsumeTimeout = "consumeTimeout";

    public static final String CheckImmunityTimeInSeconds = "CheckImmunityTimeInSeconds";

    public static final String PostSubscriptionWhenPull = "PostSubscriptionWhenPull";

    public static final String ConsumeMessageBatchMaxSize = "ConsumeMessageBatchMaxSize";

    public static final String MaxCachedMessageAmount = "maxCachedMessageAmount";

    public static final String MaxCachedMessageSizeInMiB = "maxCachedMessageSizeInMiB";

    public static final String InstanceName = "InstanceName";

    public static final String MsgTraceSwitch = "MsgTraceSwitch";

    public static final String MqttMessageId = "mqttMessageId";

    public static final String MqttMessage = "mqttMessage";

    public static final String MqttPublishRetain = "mqttRetain";

    public static final String MqttPublishDubFlag = "mqttPublishDubFlag";

    public static final String MqttSecondTopic = "mqttSecondTopic";

    public static final String MqttClientId = "clientId";

    public static final String MqttQOS = "qoslevel";

    public static final String INSTANCE_ID = "INSTANCE_ID";

    public static final String EXACTLYONCE_DELIVERY = "exactlyOnceDelivery";

    public static final String EXACTLYONCE_RM_REFRESHINTERVAL = "exactlyOnceRmRefreshInterval";

    public static final String MAX_BATCH_MESSAGE_COUNT = "maxBatchMessageCount";

    public static final String LANGUAGE_IDENTIFIER = "languageIdentifier";

}
