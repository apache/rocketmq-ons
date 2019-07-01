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
