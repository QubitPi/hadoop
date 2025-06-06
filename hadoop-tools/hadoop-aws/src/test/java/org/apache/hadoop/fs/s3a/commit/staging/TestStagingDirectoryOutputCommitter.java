/*
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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.reset;

/** Mocking test of directory committer. */
public class TestStagingDirectoryOutputCommitter
    extends StagingTestBase.JobCommitterTest<DirectoryStagingCommitter> {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStagingDirectoryOutputCommitter.class);

  @Override
  DirectoryStagingCommitter newJobCommitter() throws Exception {
    return new DirectoryStagingCommitter(getOutputPath(),
        createTaskAttemptForJob());
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, "merge");
    intercept(IllegalArgumentException.class,
        "MERGE", "committer conflict",
        this::newJobCommitter);
  }

  @Test
  public void testDefaultConflictResolution() throws Exception {
    getJob().getConfiguration().unset(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    pathIsDirectory(getMockS3A(), getOutputPath());
    verifyJobSetupAndCommit();
  }

  @Test
  public void testFailConflictResolution() throws Exception {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_FAIL);
    verifyFailureConflictOutcome();
  }

  protected void verifyFailureConflictOutcome() throws Exception {
    final S3AFileSystem mockFS = getMockS3A();
    pathIsDirectory(mockFS, getOutputPath());
    final DirectoryStagingCommitter committer = newJobCommitter();

    // this should fail
    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Should throw an exception because the path exists",
        () -> committer.setupJob(getJob()));

    // but there are no checks in job commit (HADOOP-15469)
    // this is done by calling the preCommit method directly,

    final CommitContext commitContext = new CommitOperations(getWrapperFS()).
        createCommitContext(getJob(), getOutputPath(), 0,
            IOStatisticsContext.getCurrentIOStatisticsContext());
    committer.preCommitJob(commitContext, AbstractS3ACommitter.ActiveCommit.empty());

    reset(mockFS);
    pathDoesNotExist(mockFS, getOutputPath());

    committer.setupJob(getJob());
    verifyExistenceChecked(mockFS, getOutputPath());
    verifyMkdirsInvoked(mockFS, getOutputPath());
    verifyNoMoreInteractions(mockFS);

    reset(mockFS);
    pathDoesNotExist(mockFS, getOutputPath());
    committer.commitJob(getJob());
    verifyCompletion(mockFS);
  }

  @Test
  public void testAppendConflictResolution() throws Exception {

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    FileSystem mockS3 = getMockS3A();
    pathIsDirectory(mockS3, getOutputPath());
    verifyJobSetupAndCommit();
  }

  protected void verifyJobSetupAndCommit()
      throws Exception {
    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    FileSystem mockS3 = getMockS3A();

    Mockito.reset(mockS3);
    pathExists(mockS3, getOutputPath());

    committer.commitJob(getJob());
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3A();

    pathIsDirectory(mockS3, getOutputPath());

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    final DirectoryStagingCommitter committer = newJobCommitter();

    committer.setupJob(getJob());

    Mockito.reset(mockS3);
    pathExists(mockS3, getOutputPath());
    canDelete(mockS3, getOutputPath());

    committer.commitJob(getJob());
    verifyDeleted(mockS3, getOutputPath());
    verifyCompletion(mockS3);
  }

  @Test
  public void testReplaceConflictFailsIfDestIsFile() throws Exception {
    pathIsFile(getMockS3A(), getOutputPath());

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Expected a PathExistsException as the destination"
            + " was a file",
        () -> {
          newJobCommitter().setupJob(getJob());
        });
  }

  @Test
  public void testAppendConflictFailsIfDestIsFile() throws Exception {
    pathIsFile(getMockS3A(), getOutputPath());

    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);

    intercept(PathExistsException.class,
        InternalCommitterConstants.E_DEST_EXISTS,
        "Expected a PathExistsException as a the destination"
            + " is a file",
        () -> {
          newJobCommitter().setupJob(getJob());
        });
  }

  @Test
  public void testValidateDefaultConflictMode() throws Throwable {
    Configuration baseConf = new Configuration(true);
    String[] sources = baseConf.getPropertySources(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    String sourceStr = Arrays.stream(sources)
        .collect(Collectors.joining(","));
    LOG.info("source of conflict mode {}", sourceStr);
    String baseConfVal = baseConf
        .getTrimmed(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    assertEquals(CONFLICT_MODE_APPEND, baseConfVal,
        "conflict mode in core config from " + sourceStr);
  }
}
