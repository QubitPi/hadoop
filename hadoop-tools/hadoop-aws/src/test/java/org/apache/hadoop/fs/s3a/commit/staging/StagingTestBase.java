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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.hadoop.fs.s3a.S3AInternals;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractCommitITest;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.MiniDFSClusterService;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.test.HadoopTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test base for mock tests of staging committers:
 * core constants and static methods, inner classes
 * for specific test types.
 *
 * Some of the verification methods here are unused...they are being left
 * in place in case changes on the implementation make the verifications
 * relevant again.
 */
public class StagingTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(StagingTestBase.class);

  public static final String BUCKET = MockS3AFileSystem.BUCKET;
  public static final String OUTPUT_PREFIX = "output/path";
  /** The raw bucket URI Path before any canonicalization. */
  public static final Path RAW_BUCKET_PATH =
      new Path("s3a://" + BUCKET + "/");
  /** The raw bucket URI Path before any canonicalization. */
  public static final URI RAW_BUCKET_URI =
      RAW_BUCKET_PATH.toUri();

  @SuppressWarnings("StaticNonFinalField")
  private static Path outputPath =
      new Path("s3a://" + BUCKET + "/" + OUTPUT_PREFIX);

  @SuppressWarnings("StaticNonFinalField")
  private static URI outputPathUri = getOutputPath().toUri();
  @SuppressWarnings("StaticNonFinalField")
  private static Path root;

  protected StagingTestBase() {
  }

  /**
   * Sets up the mock filesystem instance and binds it to the
   * {@link FileSystem#get(URI, Configuration)} call for the supplied URI
   * and config.
   * All standard mocking setup MUST go here.
   * @param conf config to use
   * @param outcome tuple of outcomes to store in mock FS
   * @return the filesystem created
   * @throws IOException IO problems.
   */
  protected static S3AFileSystem createAndBindMockFSInstance(Configuration conf,
      Pair<StagingTestBase.ClientResults, StagingTestBase.ClientErrors> outcome,
                                                             S3Client mockS3Client)
      throws IOException {
    S3AFileSystem mockFs = mockS3AFileSystemRobustly(mockS3Client);
    MockS3AFileSystem wrapperFS = new MockS3AFileSystem(mockFs, outcome);
    URI uri = RAW_BUCKET_URI;
    wrapperFS.initialize(uri, conf);
    root = wrapperFS.makeQualified(new Path("/"));
    outputPath = new Path(getRoot(), OUTPUT_PREFIX);
    outputPathUri = getOutputPath().toUri();
    FileSystemTestHelper.addFileSystemForTesting(uri, conf, wrapperFS);
    return mockFs;
  }

  private static S3AFileSystem mockS3AFileSystemRobustly(S3Client mockS3Client) throws IOException {

    S3AFileSystem mockFS = mock(S3AFileSystem.class);
    S3AStore store = mock(S3AStore.class);
    when(store.getOrCreateS3Client())
        .thenReturn(mockS3Client);

    S3AInternals s3AInternals = mock(S3AInternals.class);

    when(mockFS.getS3AInternals()).thenReturn(s3AInternals);

    when(s3AInternals.getStore()).thenReturn(store);
    when(s3AInternals.getAmazonS3Client(anyString()))
        .thenReturn(mockS3Client);
    doNothing().when(mockFS).incrementReadOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    return mockFS;
  }

  /**
   * Look up the FS by URI, return a (cast) Mock wrapper.
   * @param conf config
   * @return the FS
   * @throws IOException IO Failure
   */
  public static MockS3AFileSystem lookupWrapperFS(Configuration conf)
      throws IOException {
    return (MockS3AFileSystem) FileSystem.get(getOutputPathUri(), conf);
  }

  public static void verifyCompletion(FileSystem mockS3) throws IOException {
    verifyCleanupTempFiles(mockS3);
    verifyNoMoreInteractions(mockS3);
  }

  public static void verifyDeleted(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).delete(path, true);
  }

  public static void verifyDeleted(FileSystem mockS3, String child)
      throws IOException {
    verifyDeleted(mockS3, new Path(getOutputPath(), child));
  }

  public static void verifyCleanupTempFiles(FileSystem mockS3)
      throws IOException {
    verifyDeleted(mockS3,
        new Path(getOutputPath(), CommitConstants.TEMPORARY));
  }

  protected static void assertConflictResolution(
      StagingCommitter committer,
      JobContext job,
      ConflictResolution mode) {
    assertEquals(mode, committer.getConflictResolutionMode(job, new Configuration()),
        "Conflict resolution mode in " + committer);
  }

  public static void pathsExist(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      pathExists(mockS3, new Path(getOutputPath(), child));
    }
  }

  public static void pathExists(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(true);
  }

  public static void pathIsDirectory(FileSystem mockS3, Path path)
      throws IOException {
    hasFileStatus(mockS3, path,
        new FileStatus(0, true, 0, 0, 0, path));
  }

  public static void pathIsFile(FileSystem mockS3, Path path)
      throws IOException {
    pathExists(mockS3, path);
    hasFileStatus(mockS3, path,
        new FileStatus(0, false, 0, 0, 0, path));
  }

  public static void pathDoesNotExist(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(false);
    when(mockS3.getFileStatus(path)).thenThrow(
        new FileNotFoundException("mock fnfe of " + path));
  }

  public static void hasFileStatus(FileSystem mockS3,
      Path path, FileStatus status) throws IOException {
    when(mockS3.getFileStatus(path)).thenReturn(status);
  }

  public static void mkdirsHasOutcome(FileSystem mockS3,
      Path path, boolean outcome) throws IOException {
    when(mockS3.mkdirs(path)).thenReturn(outcome);
  }

  public static void canDelete(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      canDelete(mockS3, new Path(getOutputPath(), child));
    }
  }

  public static void canDelete(FileSystem mockS3, Path f) throws IOException {
    when(mockS3.delete(f,
        true /* recursive */))
        .thenReturn(true);
  }

  public static void verifyExistenceChecked(FileSystem mockS3, String child)
      throws IOException {
    verifyExistenceChecked(mockS3, new Path(getOutputPath(), child));
  }

  public static void verifyExistenceChecked(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).getFileStatus(path);
  }

  /**
   * Verify that mkdirs was invoked once.
   * @param mockS3 mock
   * @param path path to check
   * @throws IOException from the mkdirs signature.
   */
  public static void verifyMkdirsInvoked(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).mkdirs(path);
  }

  protected static URI getOutputPathUri() {
    return outputPathUri;
  }

  static Path getRoot() {
    return root;
  }

  static Path getOutputPath() {
    return outputPath;
  }

  /**
   * Provides setup/teardown of a MiniDFSCluster for tests that need one.
   */
  public static class MiniDFSTest extends HadoopTestBase {

    private static MiniDFSClusterService hdfs;

    private static JobConf conf = null;

    protected static JobConf getConfiguration() {
      return conf;
    }

    protected static FileSystem getDFS() {
      return hdfs.getClusterFS();
    }

    /**
     * Setup the mini HDFS cluster.
     * @throws IOException Failure
     */
    @BeforeAll
    @SuppressWarnings("deprecation")
    public static void setupHDFS() throws IOException {
      if (hdfs == null) {
        JobConf c = new JobConf();
        hdfs = new MiniDFSClusterService();
        hdfs.init(c);
        hdfs.start();
        conf = c;
      }
    }

    @SuppressWarnings("ThrowableNotThrown")
    @AfterAll
    public static void teardownFS() throws IOException {
      ServiceOperations.stopQuietly(hdfs);
      conf = null;
      hdfs = null;
    }

  }

  /**
   * Base class for job committer tests.
   * @param <C> committer
   */
  public abstract static class JobCommitterTest<C extends OutputCommitter>
      extends HadoopTestBase {
    private static final JobID JOB_ID = new JobID("job", 1);
    private JobConf jobConf;

    // created in BeforeClass
    private S3AFileSystem mockFS = null;
    private MockS3AFileSystem wrapperFS = null;
    private JobContext job = null;

    // created in Before
    private StagingTestBase.ClientResults results = null;
    private StagingTestBase.ClientErrors errors = null;
    private S3Client mockClient = null;

    @BeforeEach
    public void setupJob() throws Exception {
      this.jobConf = createJobConf();

      this.job = new JobContextImpl(jobConf, JOB_ID);
      this.results = new StagingTestBase.ClientResults();
      this.errors = new StagingTestBase.ClientErrors();
      this.mockClient = newMockS3Client(results, errors);
      this.mockFS = createAndBindMockFSInstance(jobConf,
          Pair.of(results, errors), mockClient);
      this.wrapperFS = lookupWrapperFS(jobConf);
      // and bind the FS
      wrapperFS.setAmazonS3Client(mockClient);
    }

    protected JobConf createJobConf() {
      JobConf conf = new JobConf();
      conf.set(InternalCommitterConstants.FS_S3A_COMMITTER_UUID,
          UUID.randomUUID().toString());
      conf.setBoolean(
          CommitConstants.CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          false);
      return conf;
    }

    public S3AFileSystem getMockS3A() {
      return mockFS;
    }

    public MockS3AFileSystem getWrapperFS() {
      return wrapperFS;
    }

    public JobContext getJob() {
      return job;
    }

    /**
     * Create a task attempt for a job by creating a stub task ID.
     * @return a task attempt
     */
    public TaskAttemptContext createTaskAttemptForJob() {
      return AbstractCommitITest.taskAttemptForJob(
          MRBuilderUtils.newJobId(1, JOB_ID.getId(), 1), job);
    }

    protected StagingTestBase.ClientResults getMockResults() {
      return results;
    }

    protected StagingTestBase.ClientErrors getMockErrors() {
      return errors;
    }

    abstract C newJobCommitter() throws Exception;
  }

  /** Abstract test of task commits. */
  public abstract static class TaskCommitterTest<C extends OutputCommitter>
      extends JobCommitterTest<C> {
    private static final TaskAttemptID AID = new TaskAttemptID(
        new TaskID(JobCommitterTest.JOB_ID, TaskType.REDUCE, 2), 3);

    private C jobCommitter = null;
    private TaskAttemptContext tac = null;
    private File tempDir;

    @BeforeEach
    public void setupTask() throws Exception {
      this.jobCommitter = newJobCommitter();
      jobCommitter.setupJob(getJob());

      this.tac = new TaskAttemptContextImpl(
          new Configuration(getJob().getConfiguration()), AID);

      // get the task's configuration copy so modifications take effect
      String tmp = System.getProperty(
          InternalCommitterConstants.JAVA_IO_TMPDIR);
      tempDir = new File(tmp);
      tac.getConfiguration().set(Constants.BUFFER_DIR, tmp + "/buffer");
      tac.getConfiguration().set(
          CommitConstants.FS_S3A_COMMITTER_STAGING_TMP_PATH,
          tmp + "/cluster");
    }

    protected C getJobCommitter() {
      return jobCommitter;
    }

    protected TaskAttemptContext getTAC() {
      return tac;
    }

    abstract C newTaskCommitter() throws Exception;

    protected File getTempDir() {
      return tempDir;
    }
  }

  /**
   * Results accrued during mock runs.
   * This data is serialized in MR Tests and read back in in the test runner
   */
  public static class ClientResults implements Serializable {
    private static final long serialVersionUID = -3137637327090709905L;
    // For inspection of what the committer did
    private final Map<String, CreateMultipartUploadRequest> requests =
        Maps.newHashMap();
    private final List<String> uploads = Lists.newArrayList();
    private final List<UploadPartRequest> parts = Lists.newArrayList();
    private final Map<String, List<String>> tagsByUpload = Maps.newHashMap();
    private final List<CompleteMultipartUploadRequest> commits =
        Lists.newArrayList();
    private final List<AbortMultipartUploadRequest> aborts
        = Lists.newArrayList();
    private final Map<String, String> activeUploads =
        Maps.newHashMap();
    private final List<DeleteObjectRequest> deletes = Lists.newArrayList();

    public Map<String, CreateMultipartUploadRequest> getRequests() {
      return requests;
    }

    public List<String> getUploads() {
      return uploads;
    }

    public List<UploadPartRequest> getParts() {
      return parts;
    }

    public Map<String, List<String>> getTagsByUpload() {
      return tagsByUpload;
    }

    public List<CompleteMultipartUploadRequest> getCommits() {
      return commits;
    }

    public List<AbortMultipartUploadRequest> getAborts() {
      return aborts;
    }

    public List<DeleteObjectRequest> getDeletes() {
      return deletes;
    }

    public List<String> getDeletePaths() {
      return deletes.stream().map(DeleteObjectRequest::key).collect(
          Collectors.toList());
    }

    public void resetDeletes() {
      deletes.clear();
    }

    public void resetUploads() {
      uploads.clear();
      activeUploads.clear();
    }

    public void resetCommits() {
      commits.clear();
    }

    public void resetRequests() {
      requests.clear();
    }

    public void addUpload(String id, String key) {
      activeUploads.put(id, key);
    }

    public void addUploads(Map<String, String> uploadMap) {
      activeUploads.putAll(uploadMap);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          super.toString());
      sb.append("{ requests=").append(requests.size());
      sb.append(", uploads=").append(uploads.size());
      sb.append(", parts=").append(parts.size());
      sb.append(", tagsByUpload=").append(tagsByUpload.size());
      sb.append(", commits=").append(commits.size());
      sb.append(", aborts=").append(aborts.size());
      sb.append(", deletes=").append(deletes.size());
      sb.append('}');
      return sb.toString();
    }
  }

  /** Control errors to raise in mock S3 client. */
  public static class ClientErrors {
    // For injecting errors
    private int failOnInit = -1;
    private int failOnUpload = -1;
    private int failOnCommit = -1;
    private int failOnAbort = -1;
    private boolean recover = false;

    public void failOnInit(int initNum) {
      this.failOnInit = initNum;
    }

    public void failOnUpload(int uploadNum) {
      this.failOnUpload = uploadNum;
    }

    public void failOnCommit(int commitNum) {
      this.failOnCommit = commitNum;
    }

    public void failOnAbort(int abortNum) {
      this.failOnAbort = abortNum;
    }

    public void recoverAfterFailure() {
      this.recover = true;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ClientErrors{");
      sb.append("failOnInit=").append(failOnInit);
      sb.append(", failOnUpload=").append(failOnUpload);
      sb.append(", failOnCommit=").append(failOnCommit);
      sb.append(", failOnAbort=").append(failOnAbort);
      sb.append(", recover=").append(recover);
      sb.append('}');
      return sb.toString();
    }

    public int getFailOnInit() {
      return failOnInit;
    }

    public int getFailOnUpload() {
      return failOnUpload;
    }

    public int getFailOnCommit() {
      return failOnCommit;
    }

    public int getFailOnAbort() {
      return failOnAbort;
    }

    public boolean isRecover() {
      return recover;
    }
  }

  /**
   * InvocationOnMock.getArgumentAt comes and goes with Mockito versions; this
   * helper method is designed to be resilient to change.
   * @param invocation invocation to query
   * @param index argument index
   * @param clazz class of return type
   * @param <T> type of return
   * @return the argument of the invocation, cast to the given type.
   */
  @SuppressWarnings("unchecked")
  private static<T> T getArgumentAt(InvocationOnMock invocation, int index,
      Class<T> clazz) {
    return (T)invocation.getArguments()[index];
  }

  /**
   * Instantiate mock client with the results and errors requested.
   * @param results results to accrue
   * @param errors when (if any) to fail
   * @return the mock client to patch in to a committer/FS instance
   */
  public static S3Client newMockS3Client(final ClientResults results,
      final ClientErrors errors) {
    S3Client mockClientV2 = mock(S3Client.class);
    final Object lock = new Object();

    // initiateMultipartUpload
    when(mockClientV2
        .createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenAnswer(invocation -> {
          LOG.debug("initiateMultipartUpload for {}", mockClientV2);
          synchronized (lock) {
            if (results.requests.size() == errors.failOnInit) {
              if (errors.recover) {
                errors.failOnInit(-1);
              }
              throw AwsServiceException.builder()
                  .message("Mock Fail on init " + results.requests.size())
                  .build();
            }
            String uploadId = UUID.randomUUID().toString();
            CreateMultipartUploadRequest req = getArgumentAt(invocation,
                0, CreateMultipartUploadRequest.class);
            results.requests.put(uploadId, req);
            results.activeUploads.put(uploadId, req.key());
            results.uploads.add(uploadId);
            return CreateMultipartUploadResponse.builder()
                .uploadId(uploadId)
                .build();
          }
        });

    // uploadPart
    when(mockClientV2.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenAnswer(invocation -> {
          LOG.debug("uploadPart for {}", mockClientV2);
          synchronized (lock) {
            if (results.parts.size() == errors.failOnUpload) {
              if (errors.recover) {
                errors.failOnUpload(-1);
              }
              LOG.info("Triggering upload failure");
              throw AwsServiceException.builder()
                  .message("Mock Fail on upload " + results.parts.size())
                  .build();
            }
            UploadPartRequest req = getArgumentAt(invocation,
                0, UploadPartRequest.class);
            results.parts.add(req);
            String etag = UUID.randomUUID().toString();
            List<String> etags = results.tagsByUpload.get(req.uploadId());
            if (etags == null) {
              etags = Lists.newArrayList();
              results.tagsByUpload.put(req.uploadId(), etags);
            }
            etags.add(etag);
            return UploadPartResponse.builder().eTag(etag).build();
          }
        });

    // completeMultipartUpload
    when(mockClientV2
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenAnswer(invocation -> {
          LOG.debug("completeMultipartUpload for {}", mockClientV2);
          synchronized (lock) {
            if (results.commits.size() == errors.failOnCommit) {
              if (errors.recover) {
                errors.failOnCommit(-1);
              }
              throw AwsServiceException.builder()
                  .message("Mock Fail on commit " + results.commits.size())
                  .build();
            }
            CompleteMultipartUploadRequest req = getArgumentAt(invocation,
                0, CompleteMultipartUploadRequest.class);
            String uploadId = req.uploadId();
            removeUpload(results, uploadId);
            results.commits.add(req);
            return CompleteMultipartUploadResponse.builder().build();
          }
        });

    // abortMultipartUpload mocking
    doAnswer(invocation -> {
      LOG.debug("abortMultipartUpload for {}", mockClientV2);
      synchronized (lock) {
        if (results.aborts.size() == errors.failOnAbort) {
          if (errors.recover) {
            errors.failOnAbort(-1);
          }
          throw AwsServiceException.builder()
              .message("Mock Fail on abort " + results.aborts.size())
              .build();
        }
        AbortMultipartUploadRequest req = getArgumentAt(invocation,
            0, AbortMultipartUploadRequest.class);
        String id = req.uploadId();
        removeUpload(results, id);
        results.aborts.add(req);
        return null;
      }
    })
        .when(mockClientV2)
        .abortMultipartUpload(any(AbortMultipartUploadRequest.class));

    // deleteObject mocking
    doAnswer(invocation -> {
      LOG.debug("deleteObject for {}", mockClientV2);
      synchronized (lock) {
        results.deletes.add(getArgumentAt(invocation,
            0, DeleteObjectRequest.class));
        return null;
      }
    })
        .when(mockClientV2)
        .deleteObject(any(DeleteObjectRequest.class));

    // to String returns the debug information
    when(mockClientV2.toString()).thenAnswer(
        invocation -> "Mock3AClient " + results + " " + errors);

    when(mockClientV2
        .listMultipartUploads(any(ListMultipartUploadsRequest.class)))
        .thenAnswer(invocation -> {
          synchronized (lock) {
            return ListMultipartUploadsResponse.builder()
                .uploads(results.activeUploads.entrySet().stream()
                    .map(e -> MultipartUpload.builder()
                            .uploadId(e.getKey())
                            .key(e.getValue())
                            .build())
                    .collect(Collectors.toList()))
                .build();
          }
        });

    return mockClientV2;
  }

  /**
   * Remove an upload from the upload map.
   * @param results result set
   * @param uploadId The upload ID to remove
   * @throws AwsServiceException with error code 404 if the id is unknown.
   */
  protected static void removeUpload(final ClientResults results,
      final String uploadId) {
    String removed = results.activeUploads.remove(uploadId);
    if (removed == null) {
      // upload doesn't exist
      throw AwsServiceException.builder()
          .message("not found " + uploadId)
          .statusCode(404)
          .build();
    }
  }

  /**
   * create files in the attempt path that should be found by
   * {@code getTaskOutput}.
   * @param relativeFiles list of files relative to address path
   * @param attemptPath attempt path
   * @param conf config for FS
   * @throws IOException on any failure
   */
  public static void createTestOutputFiles(List<String> relativeFiles,
      Path attemptPath,
      Configuration conf) throws IOException {
    //
    FileSystem attemptFS = attemptPath.getFileSystem(conf);
    attemptFS.delete(attemptPath, true);
    for (String relative : relativeFiles) {
      // 0-length files are ignored, so write at least one byte
      OutputStream out = attemptFS.create(new Path(attemptPath, relative));
      out.write(34);
      out.close();
    }
  }

}
