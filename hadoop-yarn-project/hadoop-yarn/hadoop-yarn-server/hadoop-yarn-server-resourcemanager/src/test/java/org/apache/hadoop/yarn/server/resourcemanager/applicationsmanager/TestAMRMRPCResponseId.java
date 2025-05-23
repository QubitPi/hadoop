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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAMRMRPCResponseId {

  private MockRM rm;
  ApplicationMasterService amService = null;

  @BeforeEach
  public void setUp() {
    this.rm = new MockRM();
    rm.start();
    amService = rm.getApplicationMasterService();
  }
  
  @AfterEach
  public void tearDown() {
    if (rm != null) {
      this.rm.stop();
    }
  }

  private AllocateResponse allocate(ApplicationAttemptId attemptId,
      final AllocateRequest req) throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token =
        rm.getRMContext().getRMApps().get(attemptId.getApplicationId())
          .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    return ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return amService.allocate(req);
      }
    });
  }

  @Test
  public void testARRMResponseId() throws Exception {

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = MockRMAppSubmitter.submitWithMemory(2000, rm);

    // Trigger the scheduling so the AM gets 'launched'
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

    am.registerAppAttempt();
    
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(0, 0F, null, null, null);

    AllocateResponse response =
        allocate(attempt.getAppAttemptId(), allocateRequest);
    assertEquals(1, response.getResponseId());
    assertTrue(response.getAMCommand() == null);
    allocateRequest =
        AllocateRequest.newInstance(response.getResponseId(), 0F, null, null,
          null);
    
    response = allocate(attempt.getAppAttemptId(), allocateRequest);
    assertEquals(2, response.getResponseId());
    /* try resending */
    response = allocate(attempt.getAppAttemptId(), allocateRequest);
    assertEquals(2, response.getResponseId());
    
    /** try sending old request again **/
    allocateRequest = AllocateRequest.newInstance(0, 0F, null, null, null);

    try {
      allocate(attempt.getAppAttemptId(), allocateRequest);
      fail();
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof InvalidApplicationMasterRequestException);
    }
  }
}
