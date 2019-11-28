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
import java.util.List;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceBean;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceContext;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceType;
import org.apache.rocketmq.ons.open.trace.core.dispatch.AsyncDispatcher;

public class OnsConsumeMessageHookImpl implements ConsumeMessageHook {

    private AsyncDispatcher localDispatcher;

    public OnsConsumeMessageHookImpl(AsyncDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "OnsConsumeMessageHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        OnsTraceContext onsTraceContext = new OnsTraceContext();
        context.setMqTraceContext(onsTraceContext);
        onsTraceContext.setTraceType(OnsTraceType.SubBefore);
        String userGroup = NamespaceUtil.withoutNamespace(context.getConsumerGroup(), context.getNamespace());
        onsTraceContext.setGroupName(userGroup);
        List<OnsTraceBean> beans = new ArrayList<OnsTraceBean>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);
            if (regionId == null || regionId.equals(OnsTraceConstants.default_region)) {
                // if regionId is default ,skip it
                continue;
            }
            if (traceOn != null && "false".equals(traceOn)) {
                // if trace switch is false ,skip it
                continue;
            }
            OnsTraceBean traceBean = new OnsTraceBean();

            String userTopic = NamespaceUtil.withoutNamespace(msg.getTopic(), context.getNamespace());
            traceBean.setTopic(userTopic);
            traceBean.setMsgId(msg.getMsgId());
            traceBean.setTags(msg.getTags());
            traceBean.setKeys(msg.getKeys());
            traceBean.setStoreTime(msg.getStoreTimestamp());
            traceBean.setBodyLength(msg.getStoreSize());
            traceBean.setRetryTimes(msg.getReconsumeTimes());
            onsTraceContext.setRegionId(regionId);
            beans.add(traceBean);
        }
        if (beans.size() > 0) {
            onsTraceContext.setTraceBeans(beans);
            onsTraceContext.setTimeStamp(System.currentTimeMillis());
            localDispatcher.append(onsTraceContext);
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        OnsTraceContext subBeforeContext = (OnsTraceContext) context.getMqTraceContext();
        if (subBeforeContext.getRegionId().equals(OnsTraceConstants.default_region)) {
            // if regionId is default ,skip it
            return;
        }
        if (subBeforeContext.getTraceBeans() == null || subBeforeContext.getTraceBeans().size() < 1) {
            // if subbefore bean is null ,skip it
            return;
        }
        OnsTraceContext subAfterContext = new OnsTraceContext();
        subAfterContext.setTraceType(OnsTraceType.SubAfter);
        subAfterContext.setRegionId(subBeforeContext.getRegionId());
        subAfterContext.setGroupName(subBeforeContext.getGroupName());
        subAfterContext.setRequestId(subBeforeContext.getRequestId());
        subAfterContext.setSuccess(context.isSuccess());

        int costTime = (int) ((System.currentTimeMillis() - subBeforeContext.getTimeStamp()) / context.getMsgList().size());
        subAfterContext.setCostTime(costTime);//
        subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
        String contextType = context.getProps().get(MixAll.CONSUME_CONTEXT_TYPE);
        if (contextType != null) {
            subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
        }
        localDispatcher.append(subAfterContext);
    }
}
