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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.mapreduce.JobContext;


import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingTestBase.*;

/** Mocking test of the partitioned committer. */
public class TestStagingPartitionedTaskCommit
    extends StagingTestBase.TaskCommitterTest<PartitionedStagingCommitter> {

  @Override
  PartitionedStagingCommitter newJobCommitter() throws IOException {
    return new PartitionedStagingCommitter(getOutputPath(),
        createTaskAttemptForJob());
  }

  @Override
  PartitionedStagingCommitter newTaskCommitter() throws Exception {
    return new PartitionedStagingCommitter(getOutputPath(), getTAC());
  }

  // The set of files used by this test
  private static List<String> relativeFiles = Lists.newArrayList();

  @BeforeAll
  public static void createRelativeFileList() {
    for (String dateint : Arrays.asList("20161115", "20161116")) {
      for (String hour : Arrays.asList("14", "15")) {
        String relative = "dateint=" + dateint + "/hour=" + hour +
            "/" + UUID.randomUUID().toString() + ".parquet";
        relativeFiles.add(relative);
      }
    }
  }

  @Test
  public void testBadConflictMode() throws Throwable {
    getJob().getConfiguration().set(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, "merge");
    intercept(IllegalArgumentException.class,
        "MERGE", "committer conflict", this::newJobCommitter);
  }

  @Test
  public void testDefault() throws Exception {
    JobContext job = getJob();
    job.getConfiguration().unset(
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE);
    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    assertConflictResolution(committer, job, ConflictResolution.APPEND);
  }

  @Test
  public void testFail() throws Exception {
    FileSystem mockS3 = getMockS3A();

    getTAC().getConfiguration()
        .set(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_FAIL);

    final PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test failure when one partition already exists
    reset(mockS3);
    Path existsPath = new Path(getOutputPath(), relativeFiles.get(1)).getParent();
    pathExists(mockS3, existsPath);

    intercept(PathExistsException.class, "",
        "Should complain because a partition already exists: " + existsPath,
        () -> committer.commitTask(getTAC()));

    // test success
    reset(mockS3);

    committer.commitTask(getTAC());
    verifyFilesCreated(committer);
  }

  @Test
  public void testAppend() throws Exception {
    FileSystem mockS3 = getMockS3A();

    getTAC().getConfiguration()
        .set(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(getOutputPath(), relativeFiles.get(2)).getParent());

    committer.commitTask(getTAC());
    verifyFilesCreated(committer);
  }

  /**
   * Verify that the files created matches that expected.
   * @param committer committer
   */
  protected void verifyFilesCreated(
      final PartitionedStagingCommitter committer) {
    Set<String> files = Sets.newHashSet();
    for (CreateMultipartUploadRequest request :
        getMockResults().getRequests().values()) {
      assertEquals(BUCKET, request.bucket());
      files.add(request.key());
    }
    assertThat(files)
        .describedAs("Should have the right number of uploads")
        .hasSize(relativeFiles.size());

    Set<String> expected = buildExpectedList(committer);
    assertThat(files)
        .describedAs("Should have correct paths")
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testReplace() throws Exception {
    // TODO: this committer needs to delete the data that already exists
    // This test should assert that the delete was done
    FileSystem mockS3 = getMockS3A();

    getTAC().getConfiguration()
        .set(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_REPLACE);

    PartitionedStagingCommitter committer = newTaskCommitter();

    committer.setupTask(getTAC());
    createTestOutputFiles(relativeFiles,
        committer.getTaskAttemptPath(getTAC()), getTAC().getConfiguration());

    // test success when one partition already exists
    reset(mockS3);
    pathExists(mockS3, new Path(getOutputPath(), relativeFiles.get(3)).getParent());

    committer.commitTask(getTAC());
    verifyFilesCreated(committer);
  }

  public Set<String> buildExpectedList(StagingCommitter committer) {
    Set<String> expected = Sets.newHashSet();
    boolean unique = committer.useUniqueFilenames();
    for (String relative : relativeFiles) {
      expected.add(OUTPUT_PREFIX +
          "/" +
          (unique ? Paths.addUUID(relative, committer.getUUID()) : relative));
    }
    return expected;
  }
}
