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

package org.apache.rocketmq.ons.open.trace.core.dispatch.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceContext;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceDataEncoder;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceDispatcherType;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceTransferBean;
import org.apache.rocketmq.ons.open.trace.core.dispatch.AsyncDispatcher;
import org.apache.rocketmq.remoting.RPCHook;

public class AsyncArrayDispatcher implements AsyncDispatcher {
    private final static InternalLogger CLIENT_LOG = ClientLogger.getLog();
    private final int queueSize;
    private final int batchSize;
    private DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecutor;

    private AtomicLong discardCount;
    private Thread worker;
    private ArrayBlockingQueue<OnsTraceContext> traceContextQueue;
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private String dispatcherType;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private String dispatcherId = UUID.randomUUID().toString();
    private String customizedTraceTopic;

    /**
     * Create AsyncArrayDispatcher with acl RPC hook.
     *
     * @param properties
     * @param rpcHook RPC hook only can be set with AclRPCHook
     */
    public AsyncArrayDispatcher(Properties properties, RPCHook rpcHook) {
        dispatcherType = properties.getProperty(OnsTraceConstants.TraceDispatcherType);
        this.customizedTraceTopic = properties.getProperty(OnsTraceConstants.CustomizedTraceTopic);
        int queueSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.AsyncBufferSize, "2048"));
        queueSize = 1 << (32 - Integer.numberOfLeadingZeros(queueSize - 1));
        this.queueSize = queueSize;
        batchSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxBatchNum, "1"));
        this.discardCount = new AtomicLong(0L);
        traceContextQueue = new ArrayBlockingQueue<OnsTraceContext>(1024);
        appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);

        this.traceExecutor = new ThreadPoolExecutor(//
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = TraceProducerFactory.getTraceDispatcherProducer(properties, rpcHook);
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    @Override
    public void start() throws MQClientException {
        TraceProducerFactory.registerTraceDispatcher(dispatcherId);
        this.worker = new ThreadFactoryImpl("MQ-AsyncArrayDispatcher-Thread-" + dispatcherId, true)
            .newThread(new AsyncRunnable());
        this.worker.start();
        this.registerShutDownHook();
    }

    @Override
    public void start(String nameServerAddresses) throws MQClientException {
        this.traceProducer.setNamesrvAddr(nameServerAddresses);
        this.start();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((OnsTraceContext) ctx);
        if (!result) {
            CLIENT_LOG.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        CLIENT_LOG.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.traceExecutor.shutdown();
        TraceProducerFactory.unregisterTraceDispatcher(dispatcherId);
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new ThreadFactoryImpl("ShutdownHookMQTrace").newThread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            try {
                                flush();
                            } catch (IOException e) {
                                CLIENT_LOG.error("system mqtrace hook shutdown failed ,maybe loss some trace data");
                            }
                        }
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutDownHook);
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                List<OnsTraceContext> contexts = new ArrayList<OnsTraceContext>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    OnsTraceContext context = null;
                    try {
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }
                if (contexts.size() > 0) {
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecutor.submit(request);
                } else if (AsyncArrayDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<OnsTraceContext> contextList;

        public AsyncAppenderRequest(final List<OnsTraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<OnsTraceContext>(1);
            }
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }

        public void sendTraceData(List<OnsTraceContext> contextList) {
            Map<String, List<OnsTraceTransferBean>> transBeanMap = new HashMap<String, List<OnsTraceTransferBean>>(16);
            String currentRegionId = null;
            for (OnsTraceContext context : contextList) {
                currentRegionId = context.getRegionId();
                if (currentRegionId == null || context.getTraceBeans().isEmpty()) {
                    continue;
                }
                String topic = context.getTraceBeans().get(0).getTopic();
                String key = topic + OnsTraceConstants.CONTENT_SPLITOR + currentRegionId;
                List<OnsTraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<OnsTraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                OnsTraceTransferBean traceData = OnsTraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<OnsTraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(OnsTraceConstants.CONTENT_SPLITOR));
                flushData(entry.getValue(), key[0], key[1]);
            }
        }

        private void flushData(List<OnsTraceTransferBean> transBeanList, String topic, String currentRegionId) {
            if (transBeanList.size() == 0) {
                return;
            }
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();

            for (OnsTraceTransferBean bean : transBeanList) {
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId);
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId);
            }
            transBeanList.clear();
        }

        private void sendTraceDataByMQ(Set<String> keySet, final String data, String dataTopic,
            String currentRegionId) {
            String topic = customizedTraceTopic;
            if (StringUtils.isBlank(topic)) {
                topic = OnsTraceConstants.traceTopic + currentRegionId;
            }
            final Message message = new Message(topic, data.getBytes());
            message.setKeys(keySet);

            try {
                Set<String> dataBrokerSet = getBrokerSetByTopic(dataTopic);
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), topic);
                dataBrokerSet.retainAll(traceBrokerSet);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                    }

                    @Override
                    public void onException(Throwable e) {
                        CLIENT_LOG.info("send trace data ,the traceData is: {} ", data, e);
                    }
                };
                if (dataBrokerSet.isEmpty()) {
                    //no cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.getAndIncrement();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, dataBrokerSet, callback);
                }

            } catch (Exception e) {
                CLIENT_LOG.info("send trace data,the traceData is: {}", data, e);
            }
        }

        private Set<String> getBrokerSetByTopic(String topic) {
            Set<String> brokerSet = new HashSet<String>();
            if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.PRODUCER.name()) && hostProducer != null) {
                brokerSet = tryGetMessageQueueBrokerSet(hostProducer, topic);
            }
            if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.CONSUMER.name()) && hostConsumer != null) {
                brokerSet = tryGetMessageQueueBrokerSet(hostConsumer, topic);
            }
            return brokerSet;
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            String realTopic = NamespaceUtil.wrapNamespace(producer.getDefaultMQProducer().getNamespace(), topic);
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(realTopic, new TopicPublishInfo());
                producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(realTopic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQPushConsumerImpl consumer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            try {
                String realTopic = NamespaceUtil.wrapNamespace(consumer.getDefaultMQPushConsumer().getNamespace(), topic);
                Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(realTopic);
                for (MessageQueue queue : messageQueues) {
                    brokerSet.add(queue.getBrokerName());
                }
            } catch (MQClientException e) {
                CLIENT_LOG.info("fetch message queue failed, the topic is {}", topic);
            }
            return brokerSet;
        }
    }

    public String getCustomizedTraceTopic() {
        return customizedTraceTopic;
    }

    public void setCustomizedTraceTopic(String customizedTraceTopic) {
        this.customizedTraceTopic = customizedTraceTopic;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }
}
