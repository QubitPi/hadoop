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

package org.apache.hadoop.mapreduce.v2.app.commit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitFailedEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestCommitterEventHandler {
  public static class WaitForItHandler implements EventHandler<Event> {

    private Event event = null;
    
    @Override
    public synchronized void handle(Event event) {
      this.event = event;
      notifyAll();
    }
    
    public synchronized Event getAndClearEvent() throws InterruptedException {
      if (event == null) {
        final long waitTime = 5000;
        long waitStartTime = Time.monotonicNow();
        while(event == null && Time.monotonicNow() - waitStartTime < waitTime) {
          //Wait for at most 5 sec
          wait(waitTime);
        }
      }
      Event e = event;
      event = null;
      return e;
    }
    
  }
  
  static String stagingDir = "target/test-staging/";

  @BeforeAll
  public static void setup() {    
    File dir = new File(stagingDir);
    stagingDir = dir.getAbsolutePath();
  }

  @BeforeEach
  public void cleanup() throws IOException {
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }
  
  @Test
  public void testCommitWindow() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();

    TestingJobEventHandler jeh = new TestingJobEventHandler();
    dispatcher.register(JobEventType.class, jeh);

    SystemClock clock = SystemClock.getInstance();
    AppContext appContext = mock(AppContext.class);
    ApplicationAttemptId attemptid = ApplicationAttemptId.fromString(
        "appattempt_1234567890000_0001_0");
    when(appContext.getApplicationID()).thenReturn(attemptid.getApplicationId());
    when(appContext.getApplicationAttemptId()).thenReturn(attemptid);
    when(appContext.getEventHandler()).thenReturn(
        dispatcher.getEventHandler());
    when(appContext.getClock()).thenReturn(clock);
    OutputCommitter committer = mock(OutputCommitter.class);
    TestingRMHeartbeatHandler rmhh =
        new TestingRMHeartbeatHandler();

    CommitterEventHandler ceh = new CommitterEventHandler(appContext,
        committer, rmhh);
    ceh.init(conf);
    ceh.start();

    // verify trying to commit when RM heartbeats are stale does not commit
    ceh.handle(new CommitterJobCommitEvent(null, null));
    long timeToWaitMs = 5000;
    while (rmhh.getNumCallbacks() != 1 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assertions.assertEquals(1, rmhh.getNumCallbacks(),
        "committer did not register a heartbeat callback");
    verify(committer, never()).commitJob(any(JobContext.class));
    Assertions.assertEquals(0, jeh.numCommitCompletedEvents,
        "committer should not have committed");

    // set a fresh heartbeat and verify commit completes
    rmhh.setLastHeartbeatTime(clock.getTime());
    timeToWaitMs = 5000;
    while (jeh.numCommitCompletedEvents != 1 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assertions.assertEquals(1, jeh.numCommitCompletedEvents,
        "committer did not complete commit after RM heartbeat");
    verify(committer, times(1)).commitJob(any());

    //Clean up so we can try to commit again (Don't do this at home)
    cleanup();
    
    // try to commit again and verify it goes through since the heartbeat
    // is still fresh
    ceh.handle(new CommitterJobCommitEvent(null, null));
    timeToWaitMs = 5000;
    while (jeh.numCommitCompletedEvents != 2 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assertions.assertEquals(2, jeh.numCommitCompletedEvents,
        "committer did not commit");
    verify(committer, times(2)).commitJob(any());

    ceh.stop();
    dispatcher.stop();
  }

  private static class TestingRMHeartbeatHandler
      implements RMHeartbeatHandler {
    private long lastHeartbeatTime = 0;
    private ConcurrentLinkedQueue<Runnable> callbacks =
        new ConcurrentLinkedQueue<Runnable>();

    @Override
    public long getLastHeartbeatTime() {
      return lastHeartbeatTime;
    }

    @Override
    public void runOnNextHeartbeat(Runnable callback) {
      callbacks.add(callback);
    }

    public void setLastHeartbeatTime(long timestamp) {
      lastHeartbeatTime = timestamp;
      Runnable callback = null;
      while ((callback = callbacks.poll()) != null) {
        callback.run();
      }
    }

    public int getNumCallbacks() {
      return callbacks.size();
    }
  }

  private static class TestingJobEventHandler
      implements EventHandler<JobEvent> {
    int numCommitCompletedEvents = 0;

    @Override
    public void handle(JobEvent event) {
      if (event.getType() == JobEventType.JOB_COMMIT_COMPLETED) {
        ++numCommitCompletedEvents;
      }
    }
  }

  @Test
  public void testBasic() throws Exception {
    AppContext mockContext = mock(AppContext.class);
    OutputCommitter mockCommitter = mock(OutputCommitter.class);
    Clock mockClock = mock(Clock.class);
    
    CommitterEventHandler handler = new CommitterEventHandler(mockContext, 
        mockCommitter, new TestingRMHeartbeatHandler());
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    JobContext mockJobContext = mock(JobContext.class);
    ApplicationAttemptId attemptid = ApplicationAttemptId.fromString(
        "appattempt_1234567890000_0001_0");
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(attemptid.getApplicationId()));
    
    WaitForItHandler waitForItHandler = new WaitForItHandler();
    
    when(mockContext.getApplicationID()).thenReturn(attemptid.getApplicationId());
    when(mockContext.getApplicationAttemptId()).thenReturn(attemptid);
    when(mockContext.getEventHandler()).thenReturn(waitForItHandler);
    when(mockContext.getClock()).thenReturn(mockClock);
    
    handler.init(conf);
    handler.start();
    try {
      handler.handle(new CommitterJobCommitEvent(jobId, mockJobContext));

      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      Path startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      Path endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, 
          jobId);
      Path endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, 
          jobId);

      Event e = waitForItHandler.getAndClearEvent();
      assertNotNull(e);
      assertTrue(e instanceof JobCommitCompletedEvent);
      FileSystem fs = FileSystem.get(conf);
      assertTrue(fs.exists(startCommitFile), startCommitFile.toString());
      assertTrue(fs.exists(endCommitSuccessFile), endCommitSuccessFile.toString());
      assertFalse(fs.exists(endCommitFailureFile), endCommitFailureFile.toString());
      verify(mockCommitter).commitJob(any(JobContext.class));
    } finally {
      handler.stop();
    }
  }

  @Test
  public void testFailure() throws Exception {
    AppContext mockContext = mock(AppContext.class);
    OutputCommitter mockCommitter = mock(OutputCommitter.class);
    Clock mockClock = mock(Clock.class);
    
    CommitterEventHandler handler = new CommitterEventHandler(mockContext, 
        mockCommitter, new TestingRMHeartbeatHandler());
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    JobContext mockJobContext = mock(JobContext.class);
    ApplicationAttemptId attemptid =
        ApplicationAttemptId.fromString("appattempt_1234567890000_0001_0");
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(attemptid.getApplicationId()));
    
    WaitForItHandler waitForItHandler = new WaitForItHandler();
    
    when(mockContext.getApplicationID()).thenReturn(attemptid.getApplicationId());
    when(mockContext.getApplicationAttemptId()).thenReturn(attemptid);
    when(mockContext.getEventHandler()).thenReturn(waitForItHandler);
    when(mockContext.getClock()).thenReturn(mockClock);
    
    doThrow(new YarnRuntimeException("Intentional Failure")).when(mockCommitter)
      .commitJob(any(JobContext.class));
    
    handler.init(conf);
    handler.start();
    try {
      handler.handle(new CommitterJobCommitEvent(jobId, mockJobContext));

      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      Path startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      Path endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, 
          jobId);
      Path endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, 
          jobId);

      Event e = waitForItHandler.getAndClearEvent();
      assertNotNull(e);
      assertTrue(e instanceof JobCommitFailedEvent);
      FileSystem fs = FileSystem.get(conf);
      assertTrue(fs.exists(startCommitFile));
      assertFalse(fs.exists(endCommitSuccessFile));
      assertTrue(fs.exists(endCommitFailureFile));
      verify(mockCommitter).commitJob(any(JobContext.class));
    } finally {
      handler.stop();
    }
  }
}
