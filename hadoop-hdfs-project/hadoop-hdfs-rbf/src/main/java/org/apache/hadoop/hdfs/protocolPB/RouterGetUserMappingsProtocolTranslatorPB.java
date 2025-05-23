/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpcClient;

public class RouterGetUserMappingsProtocolTranslatorPB
    extends GetUserMappingsProtocolClientSideTranslatorPB {
  private final GetUserMappingsProtocolPB rpcProxy;

  public RouterGetUserMappingsProtocolTranslatorPB(GetUserMappingsProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getGroupsForUser(user);
    }
    GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto request =
        GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
        .newBuilder().setUser(user).build();

    return asyncIpcClient(() -> rpcProxy.getGroupsForUser(null, request),
        res -> res.getGroupsList().toArray(new String[res.getGroupsCount()]),
        String[].class);
  }
}
