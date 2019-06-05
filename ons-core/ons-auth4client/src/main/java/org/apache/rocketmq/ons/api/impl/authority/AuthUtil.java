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
package org.apache.rocketmq.ons.api.impl.authority;

import java.util.Map;
import java.util.SortedMap;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;


public class AuthUtil {
    public static byte[] combineRequestContent(RemotingCommand request, SortedMap<String, String> fieldsMap) {
        try {
            StringBuilder sb = new StringBuilder("");
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                if (!SessionCredentials.Signature.equals(entry.getKey())) {
                    sb.append(entry.getValue());
                }
            }

            return AuthUtil.combineBytes(sb.toString().getBytes(SessionCredentials.CHARSET), request.getBody());
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }

    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        int size = (null != b1 ? b1.length : 0) + (null != b2 ? b2.length : 0);
        byte[] total = new byte[size];
        if (null != b1) {
            System.arraycopy(b1, 0, total, 0, b1.length);
        }
        if (null != b2) {
            System.arraycopy(b2, 0, total, b1.length, b2.length);
        }
        return total;
    }


    public static String calSignature(byte[] data, String secretKey) {
        String signature = OnsAuthSigner.calSignature(data, secretKey);
        return signature;
    }
}
