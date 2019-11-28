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
package org.apache.rocketmq.ons.open.trace.core.hook;

import java.util.ArrayList;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceBean;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceContext;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceType;
import org.apache.rocketmq.ons.open.trace.core.dispatch.AsyncDispatcher;

public class OnsClientSendMessageHookImpl implements SendMessageHook {

    private AsyncDispatcher localDispatcher;

    public OnsClientSendMessageHookImpl(AsyncDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "OnsClientSendMessageHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {

        if (context == null || context.getMessage().getTopic().startsWith(MixAll.SYSTEM_TOPIC_PREFIX)) {
            return;
        }
        OnsTraceContext onsContext = new OnsTraceContext();
        onsContext.setTraceBeans(new ArrayList<OnsTraceBean>(1));
        context.setMqTraceContext(onsContext);
        onsContext.setTraceType(OnsTraceType.Pub);
        String userGroup = NamespaceUtil.withoutNamespace(context.getProducerGroup(), context.getNamespace());
        onsContext.setGroupName(userGroup);
        OnsTraceBean traceBean = new OnsTraceBean();
        String userTopic = NamespaceUtil.withoutNamespace(context.getMessage().getTopic(), context.getNamespace());
        traceBean.setTopic(userTopic);
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        onsContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {

        if (context == null || context.getMessage().getTopic().startsWith(OnsTraceConstants.traceTopic) || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }
        if (context.getSendResult().getRegionId() == null
            || context.getSendResult().getRegionId().equals(OnsTraceConstants.default_region)
            || !context.getSendResult().isTraceOn()) {
            // if regionId is default or switch is false,skip it
            return;
        }
        OnsTraceContext onsContext = (OnsTraceContext) context.getMqTraceContext();
        OnsTraceBean traceBean = onsContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - onsContext.getTimeStamp()) / onsContext.getTraceBeans().size());
        onsContext.setCostTime(costTime);
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            onsContext.setSuccess(true);
        } else {
            onsContext.setSuccess(false);
        }
        onsContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(onsContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(onsContext);
    }
}
