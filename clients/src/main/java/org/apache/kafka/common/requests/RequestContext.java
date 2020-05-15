/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

public class RequestContext {
    public final RequestHeader header;//request头部数据
    public final String connectionId;   //request发送方的TCP连接串标识
    public final InetAddress clientAddress; //request发送方ip地址
    public final KafkaPrincipal principal;  //kafka用户认证类，用于认证授权
    public final ListenerName listenerName; //监听器名称
    public final SecurityProtocol securityProtocol; //安全协议类型

    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol) {
        this.header = header;
        this.connectionId = connectionId;
        this.clientAddress = clientAddress;
        this.principal = principal;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
    }

    //从buffer中提取对应的request对象以及他的大小
    public RequestAndSize parseRequest(ByteBuffer buffer) {
        if (isUnsupportedApiVersionsRequest()) {
            // 不支持的ApiVersion请求类型被视为V0版本的请求，并且不做解析操作
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest((short) 0, header.apiVersion());
            return new RequestAndSize(apiVersionsRequest, 0);
        } else {
            //从请求头部数据中获取APIkeys信息
            ApiKeys apiKey = header.apiKey();
            try {
                //从请求头部数据中获取版本信息
                short apiVersion = header.apiVersion();
                //解析请求
                Struct struct = apiKey.parseRequest(apiVersion, buffer);
                AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
                return new RequestAndSize(body, struct.sizeOf());
            } catch (Throwable ex) {
                throw new InvalidRequestException("Error getting request for apiKey: " + apiKey +
                        ", apiVersion: " + header.apiVersion() +
                        ", connectionId: " + connectionId +
                        ", listenerName: " + listenerName +
                        ", principal: " + principal, ex);
            }
        }
    }

    public Send buildResponse(AbstractResponse body) {
        ResponseHeader responseHeader = header.toResponseHeader();
        return body.toSend(connectionId, responseHeader, apiVersion());
    }

    private boolean isUnsupportedApiVersionsRequest() {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    public short apiVersion() {
        // Use v0 when serializing an unhandled ApiVersion response
        if (isUnsupportedApiVersionsRequest())
            return 0;
        return header.apiVersion();
    }

}
