/**
 * Copyright (C) 2010-2016 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.ons.sample.producer;

import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionChecker;
import org.apache.rocketmq.ons.api.transaction.TransactionStatus;


public class LocalTransactionCheckerImpl implements LocalTransactionChecker {

    @Override
    public TransactionStatus check(Message msg) {
        System.out.println("Receive transaction check back request, MsgId: " + msg.getMsgID());
        return TransactionStatus.CommitTransaction;
    }
}
