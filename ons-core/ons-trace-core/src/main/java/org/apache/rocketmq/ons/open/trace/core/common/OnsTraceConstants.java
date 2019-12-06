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

package org.apache.rocketmq.ons.open.trace.core.common;

import javax.annotation.Generated;
import org.apache.rocketmq.common.MixAll;

@Generated("ons-client")
public class OnsTraceConstants {

    public static final String NAMESRV_ADDR = "NAMESRV_ADDR";

    public static final String ADDRSRV_URL = "ADDRSRV_URL";

    public static final String AccessKey = "AccessKey";

    public static final String SecretKey = "SecretKey";

    public static final String InstanceName = "InstanceName";

    public static final String AsyncBufferSize = "AsyncBufferSize";

    public static final String MaxBatchNum = "MaxBatchNum";

    public static final String WakeUpNum = "WakeUpNum";

    public static final String MaxMsgSize = "MaxMsgSize";

    public static final String groupName = "_INNER_TRACE_PRODUCER";

    public static final String traceTopic = MixAll.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";

    public static final String default_region = MixAll.DEFAULT_TRACE_REGION_ID;

    public static final char CONTENT_SPLITOR = (char) 1;
    public static final char FIELD_SPLITOR = (char) 2;

    public static final String TraceDispatcherType = "DispatcherType";

    public static final String CustomizedTraceTopic = "customizedTraceTopic";
}
