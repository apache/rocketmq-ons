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

import org.apache.rocketmq.ons.api.impl.authority.AuthUtil;
import org.apache.rocketmq.ons.api.impl.authority.SessionCredentials;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.ons.api.impl.authority.SessionCredentials.AccessKey;
import static org.apache.rocketmq.ons.api.impl.authority.SessionCredentials.ONSChannelKey;
import static org.apache.rocketmq.ons.api.impl.authority.SessionCredentials.SecurityToken;
import static org.apache.rocketmq.ons.api.impl.authority.SessionCredentials.Signature;

public class ClientRPCHook extends AbstractRPCHook {
    private SessionCredentials sessionCredentials;

    public ClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        if (sessionCredentials.getAccessKey() == null && sessionCredentials.getSecretKey() == null) {
            return;
        }
        byte[] total = AuthUtil.combineRequestContent(request,
            parseRequestContent(request, sessionCredentials.getAccessKey(),
                sessionCredentials.getSecurityToken(), sessionCredentials.getOnsChannel().name()));
        String signature = AuthUtil.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(Signature, signature);
        request.addExtField(AccessKey, sessionCredentials.getAccessKey());
        request.addExtField(ONSChannelKey, sessionCredentials.getOnsChannel().name());

        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SecurityToken, sessionCredentials.getSecurityToken());
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

}
