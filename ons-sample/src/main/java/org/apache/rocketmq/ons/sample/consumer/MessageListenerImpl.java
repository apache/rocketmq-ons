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
package org.apache.rocketmq.ons.sample.consumer;

import org.apache.rocketmq.ons.api.Action;
import org.apache.rocketmq.ons.api.ConsumeContext;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.MessageListener;

public class MessageListenerImpl implements MessageListener {
    @Override
    public Action consume(Message message, ConsumeContext consumeContext) {
        System.out.printf("Receive message, Topic is: %s,  MsgId is: %s%n", message.getTopic(), message.getMsgID());
        return Action.CommitMessage;
    }
}
