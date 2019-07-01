package org.apache.rocketmq.ons.sample.producer;

import java.util.Date;
import java.util.Properties;
import org.apache.rocketmq.ons.api.Message;
import org.apache.rocketmq.ons.api.ONSFactory;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.SendResult;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.transaction.LocalTransactionExecuter;
import org.apache.rocketmq.ons.api.transaction.TransactionProducer;
import org.apache.rocketmq.ons.api.transaction.TransactionStatus;
import org.apache.rocketmq.ons.sample.MQConfig;

public class SimpleTransactionProducer {

    public static void main(String[] args) {
        Properties tranProducerProperties = new Properties();
        tranProducerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        tranProducerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        tranProducerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        tranProducerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
        LocalTransactionCheckerImpl localTransactionChecker = new LocalTransactionCheckerImpl();
        TransactionProducer transactionProducer = ONSFactory.createTransactionProducer(tranProducerProperties, localTransactionChecker);
        transactionProducer.start();

        Message message = new Message(MQConfig.TOPIC, MQConfig.TAG, "MQ send transaction message test".getBytes());

        for (int i = 0; i < 10; i++) {
            try {
                SendResult sendResult = transactionProducer.send(message, new LocalTransactionExecuter() {
                    @Override
                    public TransactionStatus execute(Message msg, Object arg) {
                        System.out.println("Execute local transaction and return TransactionStatus.");
                        return TransactionStatus.CommitTransaction;
                    }
                }, null);
                assert sendResult != null;
            } catch (ONSClientException e) {
                System.out.println(new Date() + " Send mq message failed! Topic is:" + MQConfig.TOPIC);
                e.printStackTrace();
            }
        }

        System.out.println("Send transaction message success.");
    }
}