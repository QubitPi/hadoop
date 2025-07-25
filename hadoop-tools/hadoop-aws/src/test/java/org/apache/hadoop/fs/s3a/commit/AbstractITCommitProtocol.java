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

package org.apache.hadoop.fs.s3a.commit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS_INTELLIGENT_TIERING;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter.E_SELF_GENERATED_JOB_UUID;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.E_NO_SPARK_UUID;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_UUID;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_UUID_SOURCE;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.SPARK_WRITE_UUID;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_TASKS_SUCCEEDED;
import static org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTrackerUtils.isTrackMagicCommitsInMemoryEnabled;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the job/task commit actions of an S3A Committer, including trying to
 * simulate some failure and retry conditions.
 * Derived from
 * {@code org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter}.
 *
 * This is a complex test suite as it tries to explore the full lifecycle
 * of committers, and is designed for subclassing.
 */
@SuppressWarnings({"unchecked", "unused"})
public abstract class AbstractITCommitProtocol extends AbstractCommitITest {
  private Path outDir;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractITCommitProtocol.class);

  private static final String SUB_DIR = "SUB_DIR";

  protected static final String PART_00000 = "part-m-00000";

  /**
   * Counter to guarantee that even in parallel test runs, no job has the same
   * ID.
   */

  private String jobId;

  // A random task attempt id for testing.
  private String attempt0;
  private TaskAttemptID taskAttempt0;

  private String attempt1;
  private TaskAttemptID taskAttempt1;

  private static final Text KEY_1 = new Text("key1");
  private static final Text KEY_2 = new Text("key2");
  private static final Text VAL_1 = new Text("val1");
  private static final Text VAL_2 = new Text("val2");

  /** A job to abort in test case teardown. */
  private final List<JobData> abortInTeardown = new ArrayList<>(1);

  private final StandardCommitterFactory
      standardCommitterFactory = new StandardCommitterFactory();

  private void cleanupDestDir() throws IOException {
    rmdir(this.outDir, getConfiguration());
  }

  /**
   * This must return the name of a suite which is unique to the non-abstract
   * test.
   * @return a string which must be unique and a valid path.
   */
  protected abstract String suitename();

  /**
   * Get the log; can be overridden for test case log.
   * @return a log.
   */
  public Logger log() {
    return LOG;
  }

  /**
   * Overridden method returns the suitename as well as the method name,
   * so if more than one committer test is run in parallel, paths are
   * isolated.
   * @return a name for a method, unique across the suites and test cases.
   */
  @Override
  protected String getMethodName() {
    return suitename() + "-" + super.getMethodName();
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    jobId = randomJobId();
    attempt0 = "attempt_" + jobId + "_m_000000_0";
    taskAttempt0 = TaskAttemptID.forName(attempt0);
    attempt1 = "attempt_" + jobId + "_m_000001_0";
    taskAttempt1 = TaskAttemptID.forName(attempt1);

    outDir = path(getMethodName());
    abortMultipartUploadsUnderPath(outDir);
    cleanupDestDir();
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    describe("teardown");
    abortInTeardown.forEach(this::abortJobQuietly);
    if (outDir != null) {
      try (AuditSpan span = span()) {
        abortMultipartUploadsUnderPath(outDir);
        cleanupDestDir();
      } catch (IOException e) {
        log().info("Exception during cleanup", e);
      }
    }
    S3AFileSystem fileSystem = getFileSystem();
    if (fileSystem != null) {
      log().info("Statistics for {}:\n{}", fileSystem.getUri(),
          fileSystem.getInstrumentation().dump("  ", " =  ", "\n", true));
    }

    super.teardown();
  }

  /**
   * This only looks for leakage of committer thread pools,
   * and not any other leaked threads, such as those from S3A FS instances.
   */
  @AfterAll
  public static void checkForThreadLeakage() {
    List<String> committerThreads = getCurrentThreadNames().stream()
        .filter(n -> n.startsWith(AbstractS3ACommitter.THREAD_PREFIX))
        .collect(Collectors.toList());
    Assertions.assertThat(committerThreads)
        .describedAs("Outstanding committer threads")
        .isEmpty();
  }

  /**
   * Add the specified job to the current list of jobs to abort in teardown.
   * @param jobData job data.
   */
  protected void abortInTeardown(JobData jobData) {
    abortInTeardown.add(jobData);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    bindCommitter(conf);
    return conf;
  }

  /***
   * Bind to the committer from the methods of
   * {@link #getCommitterFactoryName()} and {@link #getCommitterName()}.
   * @param conf configuration to set up
   */
  protected void bindCommitter(Configuration conf) {
    super.bindCommitter(conf, getCommitterFactoryName(), getCommitterName());
  }

  /**
   * Create a committer for a task.
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected AbstractS3ACommitter createCommitter(
      TaskAttemptContext context) throws IOException {
    return createCommitter(getOutDir(), context);
  }

  /**
   * Create a committer for a task and a given output path.
   * @param outputPath path
   * @param context task context
   * @return new committer
   * @throws IOException failure
   */
  protected abstract AbstractS3ACommitter createCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException;


  protected String getCommitterFactoryName() {
    return CommitConstants.S3A_COMMITTER_FACTORY;
  }

  protected abstract String getCommitterName();

  protected Path getOutDir() {
    return outDir;
  }

  protected String getJobId() {
    return jobId;
  }

  protected String getAttempt0() {
    return attempt0;
  }

  protected TaskAttemptID getTaskAttempt0() {
    return taskAttempt0;
  }

  protected String getAttempt1() {
    return attempt1;
  }

  protected TaskAttemptID getTaskAttempt1() {
    return taskAttempt1;
  }

  /**
   * Functional interface for creating committers, designed to allow
   * different factories to be used to create different failure modes.
   */
  @FunctionalInterface
  public interface CommitterFactory {

    /**
     * Create a committer for a task.
     * @param context task context
     * @return new committer
     * @throws IOException failure
     */
    AbstractS3ACommitter createCommitter(
        TaskAttemptContext context) throws IOException;
  }

  /**
   * The normal committer creation factory, uses the abstract methods
   * in the class.
   */
  public class StandardCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3ACommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return AbstractITCommitProtocol.this.createCommitter(context);
    }
  }

  /**
   * Write some text out.
   * @param context task
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   * @return the path written to
   */
  protected Path writeTextOutput(TaskAttemptContext context)
      throws IOException, InterruptedException {
    describe("write output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID())) {
      LoggingTextOutputFormat.LoggingLineRecordWriter<Object, Object>
          recordWriter = new LoggingTextOutputFormat<>().getRecordWriter(
          context);
      writeOutput(recordWriter,
          context);
      return recordWriter.getDest();
    }
  }

  /**
   * Write the standard output.
   * @param writer record writer
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeOutput(RecordWriter writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    try(CloseWriter cw = new CloseWriter(writer, context)) {
      writer.write(KEY_1, VAL_1);
      writer.write(null, nullWritable);
      writer.write(null, VAL_1);
      writer.write(nullWritable, VAL_2);
      writer.write(KEY_2, nullWritable);
      writer.write(KEY_1, null);
      writer.write(null, null);
      writer.write(KEY_2, VAL_2);
      writer.close(context);
    }
  }

  /**
   * Write the output of a map.
   * @param writer record writer
   * @param context task context
   * @throws IOException IO failure
   * @throws InterruptedException write interrupted
   */
  private void writeMapFileOutput(RecordWriter writer,
      TaskAttemptContext context) throws IOException, InterruptedException {
    describe("\nWrite map output");
    try (DurationInfo d = new DurationInfo(LOG,
        "Writing Text output for task %s", context.getTaskAttemptID());
         CloseWriter cw = new CloseWriter(writer, context)) {
      for (int i = 0; i < 10; ++i) {
        Text val = ((i & 1) == 1) ? VAL_1 : VAL_2;
        writer.write(new LongWritable(i), val);
      }
      writer.close(context);
    }
  }

  /**
   * Details on a job for use in {@code startJob} and elsewhere.
   */
  public static class JobData {
    private final Job job;
    private final JobContext jContext;
    private final TaskAttemptContext tContext;
    private final AbstractS3ACommitter committer;
    private final Configuration conf;
    private Path writtenTextPath; // null if not written to

    public JobData(Job job,
        JobContext jContext,
        TaskAttemptContext tContext,
        AbstractS3ACommitter committer) {
      this.job = job;
      this.jContext = jContext;
      this.tContext = tContext;
      this.committer = committer;
      conf = job.getConfiguration();
    }

    public Job getJob() {
      return job;
    }

    public JobContext getJContext() {
      return jContext;
    }

    public TaskAttemptContext getTContext() {
      return tContext;
    }

    public AbstractS3ACommitter getCommitter() {
      return committer;
    }

    public Configuration getConf() {
      return conf;
    }

    public Path getWrittenTextPath() {
      return writtenTextPath;
    }
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
   * @return the new job
   * @throws IOException failure
   */
  public Job newJob() throws IOException {
    return newJob(outDir, getConfiguration(), attempt0);
  }

  /**
   * Create a new job. Sets the task attempt ID,
   * and output dir; asks for a success marker.
   * @param dir dest dir
   * @param configuration config to get the job from
   * @param taskAttemptId task attempt
   * @return the new job
   * @throws IOException failure
   */
  private Job newJob(Path dir, Configuration configuration,
      String taskAttemptId) throws IOException {
    Job job = Job.getInstance(configuration);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptId);
    conf.setBoolean(CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    FileOutputFormat.setOutputPath(job, dir);
    return job;
  }

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
  protected JobData startJob(boolean writeText)
      throws IOException, InterruptedException {
    return startJob(standardCommitterFactory, writeText);
  }

  /**
   * Start a job with a committer; optionally write the test data.
   * Always register the job to be aborted (quietly) in teardown.
   * This is, from an "OO-purity perspective" the wrong kind of method to
   * do: it's setting things up, mixing functionality, registering for teardown.
   * Its aim is simple though: a common body of code for starting work
   * in test cases.
   * @param factory the committer factory to use
   * @param writeText should the text be written?
   * @return the job data 4-tuple
   * @throws IOException IO problems
   * @throws InterruptedException interruption during write
   */
  protected JobData startJob(CommitterFactory factory, boolean writeText)
      throws IOException, InterruptedException {
    Job job = newJob();
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    AbstractS3ACommitter committer = factory.createCommitter(tContext);

    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setup(jobData);
    abortInTeardown(jobData);

    if (writeText) {
      // write output
      jobData.writtenTextPath = writeTextOutput(tContext);
    }
    return jobData;
  }

  /**
   * Set up the job and task.
   * @param jobData job data
   * @throws IOException problems
   */
  protected void setup(JobData jobData) throws IOException {
    AbstractS3ACommitter committer = jobData.committer;
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    describe("\nsetup job");
    try (DurationInfo d = new DurationInfo(LOG,
        "setup job %s", jContext.getJobID())) {
      committer.setupJob(jContext);
    }
    setupCommitter(committer, tContext);
    describe("setup complete\n");
  }

  private void setupCommitter(
      final AbstractS3ACommitter committer,
      final TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "setup task %s", tContext.getTaskAttemptID())) {
      committer.setupTask(tContext);
    }
  }

  /**
   * Abort a job quietly.
   * @param jobData job info
   */
  protected void abortJobQuietly(JobData jobData) {
    abortJobQuietly(jobData.committer, jobData.jContext, jobData.tContext);
  }

  /**
   * Abort a job quietly: first task, then job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   */
  protected void abortJobQuietly(AbstractS3ACommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) {
    describe("\naborting task");
    try {
      committer.abortTask(tContext);
    } catch (IOException e) {
      log().warn("Exception aborting task:", e);
    }
    describe("\naborting job");
    try {
      committer.abortJob(jContext, JobStatus.State.KILLED);
    } catch (IOException e) {
      log().warn("Exception aborting job", e);
    }
  }

  /**
   * Commit up the task and then the job.
   * @param committer committer
   * @param jContext job context
   * @param tContext task context
   * @throws IOException problems
   */
  protected void commit(AbstractS3ACommitter committer,
      JobContext jContext,
      TaskAttemptContext tContext) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "committing work", jContext.getJobID())) {
      describe("\ncommitting task");
      committer.commitTask(tContext);
      describe("\ncommitting job");
      committer.commitJob(jContext);
      describe("commit complete\n");

    }
  }

  /**
   * Execute work as part of a test, after creating the job.
   * After the execution, {@link #abortJobQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param action action to execute
   * @throws Exception failure
   */
  protected void executeWork(String name, ActionToTest action)
      throws Exception {
    executeWork(name, startJob(false), action);
  }

  /**
   * Execute work as part of a test, against the created job.
   * After the execution, {@link #abortJobQuietly(JobData)} is
   * called for abort/cleanup.
   * @param name name of work (for logging)
   * @param jobData job info
   * @param action action to execute
   * @throws Exception failure
   */
  public void executeWork(String name,
      JobData jobData,
      ActionToTest action) throws Exception {
    try (DurationInfo d = new DurationInfo(LOG, "Executing %s", name)) {
      action.exec(jobData.job,
          jobData.jContext,
          jobData.tContext,
          jobData.committer);
    } finally {
      abortJobQuietly(jobData);
    }
  }

  /**
   * Verify that recovery doesn't work for these committers.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testRecoveryAndCleanup() throws Exception {
    describe("Test (Unsupported) task recovery.");
    JobData jobData = startJob(true);
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    assertNotNull(committer.getWorkPath(),
        "null workPath in committer " + committer);
    assertNotNull(committer.getOutputPath(),
        "null outputPath in committer " + committer);

    // note the task attempt path.
    Path job1TaskAttempt0Path = committer.getTaskAttemptPath(tContext);

    // Commit the task. This will promote data and metadata to where
    // job commits will pick it up on commit or abort.
    commitTask(committer, tContext);
    assertTaskAttemptPathDoesNotExist(committer, tContext);

    Configuration conf2 = jobData.job.getConfiguration();
    conf2.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
    JobContext jContext2 = new JobContextImpl(conf2, taskAttempt0.getJobID());
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2,
        taskAttempt0);
    AbstractS3ACommitter committer2 = createCommitter(tContext2);
    committer2.setupJob(tContext2);

    assertFalse(committer2.isRecoverySupported(),
        "recoverySupported in " + committer2);
    intercept(PathCommitException.class, "recover",
        () -> committer2.recoverTask(tContext2));

    // the new task attempt path is different from the first, because the
    // job attempt counter is used in the path
    final Path job2TaskAttempt0Path = committer2.getTaskAttemptPath(tContext2);
    LOG.info("Job attempt 1 task attempt path {}; attempt 2 path {}",
        job1TaskAttempt0Path, job2TaskAttempt0Path);
    assertNotEquals(job1TaskAttempt0Path,
        job2TaskAttempt0Path, "Task attempt paths must differ");

    // at this point, task attempt 0 has failed to recover
    // it should be abortable though. This will be a no-op as it already
    // committed
    describe("aborting task attempt 2; expect nothing to clean up");
    committer2.abortTask(tContext2);
    describe("Aborting job 2; expect pending commits to be aborted");
    committer2.abortJob(jContext2, JobStatus.State.KILLED);
    // now, state of system may still have pending data
    assertNoMultipartUploadsPending(outDir);

  }

  protected void assertTaskAttemptPathDoesNotExist(
      AbstractS3ACommitter committer, TaskAttemptContext context)
      throws IOException {
    Path attemptPath = committer.getTaskAttemptPath(context);
    ContractTestUtils.assertPathDoesNotExist(
        attemptPath.getFileSystem(context.getConfiguration()),
        "task attempt dir",
        attemptPath);
  }

  protected void assertJobAttemptPathDoesNotExist(
      AbstractS3ACommitter committer, JobContext context)
      throws IOException {
    Path attemptPath = committer.getJobAttemptPath(context);
    ContractTestUtils.assertPathDoesNotExist(
        attemptPath.getFileSystem(context.getConfiguration()),
        "job attempt dir",
        attemptPath);
  }

  /**
   * Verify the output of the directory.
   * That includes the {@code part-m-00000-*}
   * file existence and contents, as well as optionally, the success marker.
   * @param dir directory to scan.
   * @param expectSuccessMarker check the success marker?
   * @param expectedJobId job ID, verified if non-empty and success data loaded
   * @throws Exception failure.
   */
  private void validateContent(Path dir,
      boolean expectSuccessMarker,
      String expectedJobId) throws Exception {
    if (expectSuccessMarker) {
      SuccessData successData = verifySuccessMarker(dir, expectedJobId);
    }
    Path expectedFile = getPart0000(dir);
    log().debug("Validating content in {}", expectedFile);
    StringBuilder expectedOutput = new StringBuilder();
    expectedOutput.append(KEY_1).append('\t').append(VAL_1).append("\n");
    expectedOutput.append(VAL_1).append("\n");
    expectedOutput.append(VAL_2).append("\n");
    expectedOutput.append(KEY_2).append("\n");
    expectedOutput.append(KEY_1).append("\n");
    expectedOutput.append(KEY_2).append('\t').append(VAL_2).append("\n");
    String output = readFile(expectedFile);
    assertEquals(expectedOutput.toString(), output,
        "Content of " + expectedFile);
  }

  /**
   * Verify storage class of output file matches the expected storage class.
   * @param dir output directory.
   * @param expectedStorageClass expected storage class value.
   * @throws Exception failure.
   */
  private void validateStorageClass(Path dir, String expectedStorageClass) throws Exception {
    Path expectedFile = getPart0000(dir);
    String actualStorageClass = getS3AInternals().getObjectMetadata(expectedFile)
        .storageClassAsString();

    Assertions.assertThat(actualStorageClass)
        .describedAs("Storage class of object %s", expectedFile)
        .isEqualToIgnoringCase(expectedStorageClass);
  }

  /**
   * Identify any path under the directory which begins with the
   * {@code "part-m-00000"} sequence.
   * @param dir directory to scan
   * @return the full path
   * @throws FileNotFoundException the path is missing.
   * @throws Exception failure.
   */
  protected Path getPart0000(final Path dir) throws Exception {
    final FileSystem fs = dir.getFileSystem(getConfiguration());
    FileStatus[] statuses = fs.listStatus(dir,
        path -> path.getName().startsWith(PART_00000));
    if (statuses.length != 1) {
      // fail, with a listing of the parent dir
      ContractTestUtils.assertPathExists(fs, "Output file",
          new Path(dir, PART_00000));
    }
    return statuses[0].getPath();
  }

  /**
   * Look for the partFile subdir of the output dir.
   * @param fs filesystem
   * @param dir output dir
   * @throws Exception failure.
   */
  private void validateMapFileOutputContent(
      FileSystem fs, Path dir) throws Exception {
    // map output is a directory with index and data files
    assertPathExists("Map output", dir);
    Path expectedMapDir = getPart0000(dir);
    assertPathExists("Map output", expectedMapDir);
    assertIsDirectory(expectedMapDir);
    FileStatus[] files = fs.listStatus(expectedMapDir);
    Assertions.assertThat(files)
        .describedAs("Files found in " + expectedMapDir)
        .hasSizeGreaterThan(0);
    assertPathExists("index file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.INDEX_FILE_NAME));
    assertPathExists("data file in " + expectedMapDir,
        new Path(expectedMapDir, MapFile.DATA_FILE_NAME));
  }

  /**
   * Dump all MPUs in the filesystem.
   * @throws IOException IO failure
   */
  protected void dumpMultipartUploads() throws IOException {
    countMultipartUploads("");
  }

  /**
   * Full test of the expected lifecycle: start job, task, write, commit task,
   * commit job.
   * @throws Exception on a failure
   */
  @Test
  public void testCommitLifecycle() throws Exception {
    describe("Full test of the expected lifecycle:\n" +
        " start job, task, write, commit task, commit job.\n" +
        "Verify:\n" +
        "* no files are visible after task commit\n" +
        "* the expected file is visible after job commit\n" +
        "* no outstanding MPUs after job commit");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    validateTaskAttemptWorkingDirectory(committer, tContext);

    // write output
    describe("1. Writing output");
    writeTextOutput(tContext);

    dumpMultipartUploads();
    describe("2. Committing task");
    assertTrue(committer.needsTaskCommit(tContext),
        "No files to commit were found by " + committer);
    commitTask(committer, tContext);

    // this is only task commit; there MUST be no part- files in the dest dir

    try {
      applyLocatedFiles(getFileSystem().listFiles(outDir, false),
          (status) -> Assertions.assertThat(status.getPath().toString())
              .describedAs("task committed file to dest :" + status)
              .doesNotContain("part"));
    } catch (FileNotFoundException ignored) {
      log().info("Outdir {} is not created by task commit phase ",
          outDir);
    }

    describe("3. Committing job");
    assertMultipartUploadsPending(outDir);
    commitJob(committer, jContext);

    // validate output
    describe("4. Validating content");
    validateContent(outDir, shouldExpectSuccessMarker(),
        committer.getUUID());
    assertNoMultipartUploadsPending(outDir);
  }

  @Test
  public void testCommitWithStorageClassConfig() throws Exception {
    describe("Commit with specific storage class configuration;" +
        " expect the final file has correct storage class.");

    Configuration conf = getConfiguration();
    skipIfStorageClassTestsDisabled(conf);
    conf.set(STORAGE_CLASS, STORAGE_CLASS_INTELLIGENT_TIERING);

    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    validateTaskAttemptWorkingDirectory(committer, tContext);

    // write output
    writeTextOutput(tContext);

    // commit task
    dumpMultipartUploads();
    commitTask(committer, tContext);

    // commit job
    assertMultipartUploadsPending(outDir);
    commitJob(committer, jContext);

    // validate output
    validateContent(outDir, shouldExpectSuccessMarker(),
        committer.getUUID());
    assertNoMultipartUploadsPending(outDir);

    // validate storage class
    validateStorageClass(outDir, STORAGE_CLASS_INTELLIGENT_TIERING);
  }

  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    describe("Call a task then job commit twice;" +
        "expect the second task commit to fail.");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    commit(committer, jContext, tContext);

    // validate output
    validateContent(outDir, shouldExpectSuccessMarker(),
        committer.getUUID());

    assertNoMultipartUploadsPending(outDir);

    // commit task to fail on retry
    // FNFE is not thrown in case of Magic committer when
    // in memory commit data is enabled and hence skip the check.
    boolean skipExpectFNFE = committer instanceof MagicS3GuardCommitter &&
        isTrackMagicCommitsInMemoryEnabled(tContext.getConfiguration());

    if (!skipExpectFNFE) {
      expectFNFEonTaskCommit(committer, tContext);
    }
  }

  /**
   * HADOOP-17258. If a second task attempt is committed, it
   * must succeed, and the output of the first TA, even if already
   * committed, MUST NOT be visible in the final output.
   * <p></p>
   * What's important is not just that only one TA must succeed,
   * but it must be the last one executed. Why? because that's
   * the one
   */
  @Test
  public void testTwoTaskAttemptsCommit() throws Exception {
    describe("Commit two task attempts;" +
        " expect the second attempt to succeed.");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    // do commit
    describe("\ncommitting task");
    // write output for TA 1,
    Path outputTA1 = writeTextOutput(tContext);

    // speculatively execute committer 2.

    // jobconf with a different base to its parts.
    Configuration conf2 = jobData.conf;
    conf2.set("mapreduce.output.basename", "attempt2");
    String attempt2 = "attempt_" + jobId + "_m_000000_1";
    TaskAttemptID ta2 = TaskAttemptID.forName(attempt2);
    TaskAttemptContext tContext2 = new TaskAttemptContextImpl(
        conf2, ta2);

    AbstractS3ACommitter committer2 = standardCommitterFactory
        .createCommitter(tContext2);
    setupCommitter(committer2, tContext2);
    // write output for TA 2,
    Path outputTA2 = writeTextOutput(tContext2);

    // verify the names are different.
    String name1 = outputTA1.getName();
    String name2 = outputTA2.getName();
    Assertions.assertThat(name1)
        .describedAs("name of task attempt output %s", outputTA1)
        .isNotEqualTo(name2);

    // commit task 1
    committer.commitTask(tContext);

    // then pretend that task1 didn't respond, so
    // commit task 2
    committer2.commitTask(tContext2);

    // and the job
    committer2.commitJob(tContext);

    // validate output
    S3AFileSystem fs = getFileSystem();
    SuccessData successData = validateSuccessFile(outDir, "", fs, "query", 1,
        "");
    Assertions.assertThat(successData.getFilenames())
        .describedAs("Files committed")
        .hasSize(1);

    assertPathExists("attempt2 output", new Path(outDir, name2));
    assertPathDoesNotExist("attempt1 output", new Path(outDir, name1));

    assertNoMultipartUploadsPending(outDir);
  }

  protected boolean shouldExpectSuccessMarker() {
    return true;
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithFailure() throws Exception {
    describe("Fail the first job commit then retry");
    JobData jobData = startJob(new FailingCommitterFactory(), true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);

    // now fail job
    expectSimulatedFailureOnJobCommit(jContext, committer);

    commitJob(committer, jContext);

    // but the data got there, due to the order of operations.
    validateContent(outDir, shouldExpectSuccessMarker(),
        committer.getUUID());
    expectJobCommitToFail(jContext, committer);
  }

  /**
   * Override point: the failure expected on the attempt to commit a failed
   * job.
   * @param jContext job context
   * @param committer committer
   * @throws Exception any unexpected failure.
   */
  protected void expectJobCommitToFail(JobContext jContext,
      AbstractS3ACommitter committer) throws Exception {
    // next attempt will fail as there is no longer a directory to commit
    expectJobCommitFailure(jContext, committer,
        FileNotFoundException.class);
  }

  /**
   * Expect a job commit operation to fail with a specific exception.
   * @param jContext job context
   * @param committer committer
   * @param clazz class of exception
   * @return the caught exception
   * @throws Exception any unexpected failure.
   */
  protected static <E extends IOException> E expectJobCommitFailure(
      JobContext jContext,
      AbstractS3ACommitter committer,
      Class<E> clazz)
      throws Exception {

    return intercept(clazz,
        () -> {
          committer.commitJob(jContext);
          return committer.toString();
        });
  }

  protected static void expectFNFEonTaskCommit(
      AbstractS3ACommitter committer,
      TaskAttemptContext tContext) throws Exception {
    intercept(FileNotFoundException.class,
        () -> {
          committer.commitTask(tContext);
          return committer.toString();
        });
  }

  /**
   * Simulate a failure on the first job commit; expect the
   * second to succeed.
   */
  @Test
  public void testCommitterWithNoOutputs() throws Exception {
    describe("Have a task and job with no outputs: expect success");
    JobData jobData = startJob(new FailingCommitterFactory(), false);
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do commit
    committer.commitTask(tContext);
    assertTaskAttemptPathDoesNotExist(committer, tContext);
  }

  protected static void expectSimulatedFailureOnJobCommit(JobContext jContext,
      AbstractS3ACommitter committer) throws Exception {
    ((CommitterFaultInjection) committer).setFaults(
        CommitterFaultInjection.Faults.commitJob);
    expectJobCommitFailure(jContext, committer,
        CommitterFaultInjectionImpl.Failure.class);
  }

  @Test
  public void testMapFileOutputCommitter() throws Exception {
    describe("Test that the committer generates map output into a directory\n" +
        "starting with the prefix part-");
    JobData jobData = startJob(false);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;
    Configuration conf = jobData.conf;

    // write output
    writeMapFileOutput(new MapFileOutputFormat().getRecordWriter(tContext),
        tContext);

    // do commit
    commit(committer, jContext, tContext);
    S3AFileSystem fs = getFileSystem();

    lsR(fs, outDir, true);
    String ls = ls(outDir);
    describe("\nvalidating");

    // validate output
    verifySuccessMarker(outDir, committer.getUUID());

    describe("validate output of %s", outDir);
    validateMapFileOutputContent(fs, outDir);

    // Ensure getReaders call works and also ignores
    // hidden filenames (_ or . prefixes)
    describe("listing");
    FileStatus[] filtered = fs.listStatus(outDir, HIDDEN_FILE_FILTER);
    Assertions.assertThat(filtered)
        .describedAs("listed children under " + ls)
        .hasSize(1);
    FileStatus fileStatus = filtered[0];
    Assertions.assertThat(fileStatus.getPath().getName())
        .describedAs("Not a part file: " + fileStatus)
        .startsWith(PART_00000);

    describe("getReaders()");
    Assertions.assertThat(getReaders(fs, outDir, conf))
        .describedAs("Number of MapFile.Reader entries with shared FS %s: %s",
            outDir, ls)
        .hasSize(1);

    describe("getReaders(new FS)");
    FileSystem fs2 = FileSystem.get(outDir.toUri(), conf);
    Assertions.assertThat(getReaders(fs2, outDir, conf))
        .describedAs("Number of MapFile.Reader entries with shared FS2 %s: %s",
            outDir, ls)
        .hasSize(1);

    describe("MapFileOutputFormat.getReaders");

    Assertions.assertThat(MapFileOutputFormat.getReaders(outDir, conf))
        .describedAs("Number of MapFile.Reader entries with new FS in %s: %s",
            outDir, ls)
        .hasSize(1);

  }

  /** Open the output generated by this format. */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private static MapFile.Reader[] getReaders(FileSystem fs,
      Path dir,
      Configuration conf) throws IOException {
    Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, HIDDEN_FILE_FILTER));

    // sort names, so that hash partitioning works
    Arrays.sort(names);

    MapFile.Reader[] parts = new MapFile.Reader[names.length];
    for (int i = 0; i < names.length; i++) {
      parts[i] = new MapFile.Reader(names[i], conf);
    }
    return parts;
  }

  /**
   * A functional interface which an action to test must implement.
   */
  @FunctionalInterface
  public interface ActionToTest {
    void exec(Job job, JobContext jContext, TaskAttemptContext tContext,
        AbstractS3ACommitter committer) throws Exception;
  }

  @Test
  public void testAbortTaskNoWorkDone() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) ->
            committer.abortTask(tContext));
  }

  @Test
  public void testAbortJobNoWorkDone() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) ->
            committer.abortJob(jContext, JobStatus.State.RUNNING));
  }

  @Test
  public void testCommitJobButNotTask() throws Exception {
    executeWork("commit a job while a task's work is pending, " +
            "expect task writes to be cancelled.",
        (job, jContext, tContext, committer) -> {
          // step 1: write the text
          writeTextOutput(tContext);
          // step 2: commit the job
          createCommitter(tContext).commitJob(tContext);
          // verify that no output can be observed
          assertPart0000DoesNotExist(outDir);
          // that includes, no pending MPUs; commitJob is expected to
          // cancel any.
          assertNoMultipartUploadsPending(outDir);
        }
    );
  }

  @Test
  public void testAbortTaskThenJob() throws Exception {
    JobData jobData = startJob(true);
    AbstractS3ACommitter committer = jobData.committer;

    // do abort
    committer.abortTask(jobData.tContext);

    intercept(FileNotFoundException.class, "",
        () -> getPart0000(committer.getWorkPath()));

    committer.abortJob(jobData.jContext, JobStatus.State.FAILED);
    assertJobAbortCleanedUp(jobData);

  }

  /**
   * Extension point: assert that the job was all cleaned up after an abort.
   * Base assertions
   * <ul>
   *   <li>Output dir is absent or, if present, empty</li>
   *   <li>No pending MPUs to/under the output dir</li>
   * </ul>
   * @param jobData job data
   * @throws Exception failure
   */
  public void assertJobAbortCleanedUp(JobData jobData) throws Exception {
    // special handling of magic directory; harmless in staging
    S3AFileSystem fs = getFileSystem();
    try {
      FileStatus[] children = listChildren(fs, outDir);
      if (children.length != 0) {
        lsR(fs, outDir, true);
      }
      assertArrayEquals(new FileStatus[0], children,
          "Output directory not empty " + ls(outDir));
    } catch (FileNotFoundException e) {
      // this is a valid failure mode; it means the dest dir doesn't exist yet.
    }
    assertNoMultipartUploadsPending(outDir);
  }

  @Test
  public void testFailAbort() throws Exception {
    describe("Abort the task, then job (failed), abort the job again");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;
    AbstractS3ACommitter committer = jobData.committer;

    // do abort
    committer.abortTask(tContext);

    committer.getJobAttemptPath(jContext);
    committer.getTaskAttemptPath(tContext);
    assertPart0000DoesNotExist(outDir);
    assertSuccessMarkerDoesNotExist(outDir);
    describe("Aborting job into %s", outDir);

    committer.abortJob(jContext, JobStatus.State.FAILED);

    assertTaskAttemptPathDoesNotExist(committer, tContext);
    assertJobAttemptPathDoesNotExist(committer, jContext);

    // try again; expect abort to be idempotent.
    committer.abortJob(jContext, JobStatus.State.FAILED);
    assertNoMultipartUploadsPending(outDir);

  }

  public void assertPart0000DoesNotExist(Path dir) throws Exception {
    intercept(FileNotFoundException.class,
        () -> getPart0000(dir));
    assertPathDoesNotExist("expected output file", new Path(dir, PART_00000));
  }

  @Test
  public void testAbortJobNotTask() throws Exception {
    executeWork("abort task no work",
        (job, jContext, tContext, committer) -> {
          // write output
          writeTextOutput(tContext);
          committer.abortJob(jContext, JobStatus.State.RUNNING);
          assertTaskAttemptPathDoesNotExist(
              committer, tContext);
          assertJobAttemptPathDoesNotExist(
              committer, jContext);
          assertNoMultipartUploadsPending(outDir);
        });
  }

  /**
   * This looks at what happens with concurrent commits.
   * However, the failure condition it looks for (subdir under subdir)
   * is the kind of failure you see on a rename-based commit.
   *
   * What it will not detect is the fact that both tasks will each commit
   * to the destination directory. That is: whichever commits last wins.
   *
   * There's no way to stop this. Instead it is a requirement that the task
   * commit operation is only executed when the committer is happy to
   * commit only those tasks which it knows have succeeded, and abort those
   * which have not.
   * @throws Exception failure
   */
  @Test
  public void testConcurrentCommitTaskWithSubDir() throws Exception {
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    final Configuration conf = job.getConfiguration();

    final JobContext jContext =
        new JobContextImpl(conf, taskAttempt0.getJobID());
    AbstractS3ACommitter amCommitter = createCommitter(
        new TaskAttemptContextImpl(conf, taskAttempt0));
    amCommitter.setupJob(jContext);

    final TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
    taCtx[0] = new TaskAttemptContextImpl(conf, taskAttempt0);
    taCtx[1] = new TaskAttemptContextImpl(conf, taskAttempt1);

    final TextOutputFormat[] tof = new LoggingTextOutputFormat[2];
    for (int i = 0; i < tof.length; i++) {
      tof[i] = new LoggingTextOutputFormat() {
        @Override
        public Path getDefaultWorkFile(
            TaskAttemptContext context,
            String extension) throws IOException {
          final AbstractS3ACommitter foc = (AbstractS3ACommitter)
              getOutputCommitter(context);
          return new Path(new Path(foc.getWorkPath(), SUB_DIR),
              getUniqueFile(context, getOutputName(context), extension));
        }
      };
    }

    final ExecutorService executor = HadoopExecutors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < taCtx.length; i++) {
        final int taskIdx = i;
        executor.submit(() -> {
          final OutputCommitter outputCommitter =
              tof[taskIdx].getOutputCommitter(taCtx[taskIdx]);
          outputCommitter.setupTask(taCtx[taskIdx]);
          final RecordWriter rw =
              tof[taskIdx].getRecordWriter(taCtx[taskIdx]);
          writeOutput(rw, taCtx[taskIdx]);
          describe("Committing Task %d", taskIdx);
          outputCommitter.commitTask(taCtx[taskIdx]);
          return null;
        });
      }
    } finally {
      executor.shutdown();
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        log().info("Awaiting thread termination!");
      }
    }

    // if we commit here then all tasks will be committed, so there will
    // be contention for that final directory: both parts will go in.

    describe("\nCommitting Job");
    amCommitter.commitJob(jContext);
    assertPathExists("base output directory", outDir);
    assertPart0000DoesNotExist(outDir);
    Path outSubDir = new Path(outDir, SUB_DIR);
    assertPathDoesNotExist("Must not end up with sub_dir/sub_dir",
        new Path(outSubDir, SUB_DIR));

    // validate output
    // There's no success marker in the subdirectory
    validateContent(outSubDir, false, "");
  }

  /**
   * Create a committer which fails; the class
   * {@link CommitterFaultInjectionImpl} implements the logic.
   * @param tContext task context
   * @return committer instance
   * @throws IOException failure to instantiate
   */
  protected abstract AbstractS3ACommitter createFailingCommitter(
      TaskAttemptContext tContext) throws IOException;

  /**
   * Factory for failing committers.
   */
  public class FailingCommitterFactory implements CommitterFactory {
    @Override
    public AbstractS3ACommitter createCommitter(TaskAttemptContext context)
        throws IOException {
      return createFailingCommitter(context);
    }
  }

  @Test
  public void testOutputFormatIntegration() throws Throwable {
    Configuration conf = getConfiguration();
    Job job = newJob();
    job.setOutputFormatClass(LoggingTextOutputFormat.class);
    conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    JobContext jContext = new JobContextImpl(conf, taskAttempt0.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    LoggingTextOutputFormat outputFormat = (LoggingTextOutputFormat)
        ReflectionUtils.newInstance(tContext.getOutputFormatClass(), conf);
    AbstractS3ACommitter committer = (AbstractS3ACommitter)
        outputFormat.getOutputCommitter(tContext);

    // setup
    JobData jobData = new JobData(job, jContext, tContext, committer);
    setup(jobData);
    abortInTeardown(jobData);
    LoggingTextOutputFormat.LoggingLineRecordWriter recordWriter
        = outputFormat.getRecordWriter(tContext);
    IntWritable iw = new IntWritable(1);
    recordWriter.write(iw, iw);
    long expectedLength = 4;
    Path dest = recordWriter.getDest();
    validateTaskAttemptPathDuringWrite(dest, expectedLength, jobData.getCommitter().getUUID());
    recordWriter.close(tContext);
    // at this point
    // Skip validation when commit data is stored in memory
    if (!isTrackMagicCommitsInMemoryEnabled(conf)) {
      validateTaskAttemptPathAfterWrite(dest, expectedLength);
    }
    assertTrue(committer.needsTaskCommit(tContext),
        "Committer does not have data to commit " + committer);
    commitTask(committer, tContext);
    // at this point the committer tasks stats should be current.
    IOStatisticsSnapshot snapshot = new IOStatisticsSnapshot(
        committer.getIOStatistics());
    String commitsCompleted = COMMITTER_TASKS_SUCCEEDED.getSymbol();
    assertThatStatisticCounter(snapshot, commitsCompleted)
        .describedAs("task commit count")
        .isEqualTo(1L);


    commitJob(committer, jContext);
    LOG.info("committer iostatistics {}",
        ioStatisticsSourceToString(committer));

    // validate output
    SuccessData successData = verifySuccessMarker(outDir, committer.getUUID());

    // the task commit count should get through the job commit
    IOStatisticsSnapshot successStats = successData.getIOStatistics();
    LOG.info("loaded statistics {}", successStats);
    assertThatStatisticCounter(successStats, commitsCompleted)
        .describedAs("task commit count")
        .isEqualTo(1L);
  }

  /**
   * Create a committer through reflection then use it to abort
   * a task. This mimics the action of an AM when a container fails and
   * the AM wants to abort the task attempt.
   */
  @Test
  public void testAMWorkflow() throws Throwable {
    describe("Create a committer with a null output path & use as an AM");
    JobData jobData = startJob(true);
    JobContext jContext = jobData.jContext;
    TaskAttemptContext tContext = jobData.tContext;

    TaskAttemptContext newAttempt = taskAttemptForJob(
        MRBuilderUtils.newJobId(1, 1, 1), jContext);
    Configuration conf = jContext.getConfiguration();

    // bind
    LoggingTextOutputFormat.bind(conf);

    OutputFormat<?, ?> outputFormat
        = ReflectionUtils.newInstance(newAttempt
        .getOutputFormatClass(), conf);
    Path outputPath = FileOutputFormat.getOutputPath(newAttempt);
    assertNotNull(outputPath, "null output path in new task attempt");

    AbstractS3ACommitter committer2 = (AbstractS3ACommitter)
        outputFormat.getOutputCommitter(newAttempt);
    committer2.abortTask(tContext);

    assertNoMultipartUploadsPending(getOutDir());
  }


  @Test
  public void testParallelJobsToAdjacentPaths() throws Throwable {

    describe("Run two jobs in parallel, assert they both complete");
    JobData jobData = startJob(true);
    Job job1 = jobData.job;
    AbstractS3ACommitter committer1 = jobData.committer;
    JobContext jContext1 = jobData.jContext;
    TaskAttemptContext tContext1 = jobData.tContext;

    // now build up a second job
    String jobId2 = randomJobId();
    String attempt20 = "attempt_" + jobId2 + "_m_000000_0";
    TaskAttemptID taskAttempt20 = TaskAttemptID.forName(attempt20);
    String attempt21 = "attempt_" + jobId2 + "_m_000001_0";
    TaskAttemptID taskAttempt21 = TaskAttemptID.forName(attempt21);

    Path job1Dest = outDir;
    Path job2Dest = new Path(getOutDir().getParent(),
        getMethodName() + "job2Dest");
    // little safety check
    assertNotEquals(job1Dest, job2Dest);

    // create the second job
    Job job2 = newJob(job2Dest,
        unsetUUIDOptions(new JobConf(getConfiguration())),
        attempt20);
    Configuration conf2 = job2.getConfiguration();
    conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    try {
      JobContext jContext2 = new JobContextImpl(conf2,
          taskAttempt20.getJobID());
      TaskAttemptContext tContext2 =
          new TaskAttemptContextImpl(conf2, taskAttempt20);
      AbstractS3ACommitter committer2 = createCommitter(job2Dest, tContext2);
      JobData jobData2 = new JobData(job2, jContext2, tContext2, committer2);
      setup(jobData2);
      abortInTeardown(jobData2);
      // make sure the directories are different
      assertNotEquals(committer1.getOutputPath(),
          committer2.getOutputPath(), "Committer output paths");

      assertNotEquals(committer1.getUUID(),
          committer2.getUUID(), "job UUIDs");

      // job2 setup, write some data there
      writeTextOutput(tContext2);

      // at this point, job1 and job2 both have uncommitted tasks

      // commit tasks in order task 2, task 1.
      commitTask(committer2, tContext2);
      commitTask(committer1, tContext1);

      assertMultipartUploadsPending(job1Dest);
      assertMultipartUploadsPending(job2Dest);

      // commit jobs in order job 1, job 2
      commitJob(committer1, jContext1);
      assertNoMultipartUploadsPending(job1Dest);
      getPart0000(job1Dest);
      assertMultipartUploadsPending(job2Dest);

      commitJob(committer2, jContext2);
      getPart0000(job2Dest);
      assertNoMultipartUploadsPending(job2Dest);
    } finally {
      // uncommitted files to this path need to be deleted in tests which fail
      abortMultipartUploadsUnderPath(job2Dest);
    }

  }


  /**
   * Run two jobs with the same destination and different output paths.
   * <p></p>
   * This only works if the jobs are set to NOT delete all outstanding
   * uploads under the destination path.
   * <p></p>
   * See HADOOP-17318.
   */
  @Test
  public void testParallelJobsToSameDestination() throws Throwable {

    describe("Run two jobs to the same destination, assert they both complete");
    Configuration conf = getConfiguration();
    conf.setBoolean(FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS, false);

    // this job has a job ID generated and set as the spark UUID;
    // the config is also set to require it.
    // This mimics the Spark setup process.

    String stage1Id = UUID.randomUUID().toString();
    conf.set(SPARK_WRITE_UUID, stage1Id);
    conf.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);

    // create the job and write data in its task attempt
    JobData jobData = startJob(true);
    Job job1 = jobData.job;
    AbstractS3ACommitter committer1 = jobData.committer;
    JobContext jContext1 = jobData.jContext;
    TaskAttemptContext tContext1 = jobData.tContext;
    Path job1TaskOutputFile = jobData.writtenTextPath;

    // the write path
    Assertions.assertThat(committer1.getWorkPath().toString())
        .describedAs("Work path path of %s", committer1)
        .contains(stage1Id);
    // now build up a second job
    String jobId2 = randomJobId();

    // second job will use same ID
    String attempt2 = taskAttempt0.toString();
    TaskAttemptID taskAttempt2 = taskAttempt0;

    // create the second job
    Configuration c2 = unsetUUIDOptions(new JobConf(conf));
    c2.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);
    Job job2 = newJob(outDir,
        c2,
        attempt2);
    Configuration jobConf2 = job2.getConfiguration();
    jobConf2.set("mapreduce.output.basename", "task2");
    String stage2Id = UUID.randomUUID().toString();
    jobConf2.set(SPARK_WRITE_UUID,
        stage2Id);

    JobContext jContext2 = new JobContextImpl(jobConf2,
        taskAttempt2.getJobID());
    TaskAttemptContext tContext2 =
        new TaskAttemptContextImpl(jobConf2, taskAttempt2);
    AbstractS3ACommitter committer2 = createCommitter(outDir, tContext2);
    Assertions.assertThat(committer2.getJobAttemptPath(jContext2))
        .describedAs("Job attempt path of %s", committer2)
        .isNotEqualTo(committer1.getJobAttemptPath(jContext1));
    Assertions.assertThat(committer2.getTaskAttemptPath(tContext2))
        .describedAs("Task attempt path of %s", committer2)
        .isNotEqualTo(committer1.getTaskAttemptPath(tContext1));
    Assertions.assertThat(committer2.getWorkPath().toString())
        .describedAs("Work path path of %s", committer2)
        .isNotEqualTo(committer1.getWorkPath().toString())
        .contains(stage2Id);
    Assertions.assertThat(committer2.getUUIDSource())
        .describedAs("UUID source of %s", committer2)
        .isEqualTo(AbstractS3ACommitter.JobUUIDSource.SparkWriteUUID);
    JobData jobData2 = new JobData(job2, jContext2, tContext2, committer2);
    setup(jobData2);
    abortInTeardown(jobData2);

    // the sequence is designed to ensure that job2 has active multipart
    // uploads during/after job1's work

    // if the committer is a magic committer, MPUs start in the write,
    // otherwise in task commit.
    boolean multipartInitiatedInWrite =
        committer2 instanceof MagicS3GuardCommitter;

    // job2. Here we start writing a file and have that write in progress
    // when job 1 commits.

    LoggingTextOutputFormat.LoggingLineRecordWriter<Object, Object>
        recordWriter2 = new LoggingTextOutputFormat<>().getRecordWriter(
            tContext2);

    LOG.info("Commit Task 1");
    commitTask(committer1, tContext1);

    if (multipartInitiatedInWrite) {
      // magic committer runs -commit job1 while a job2 TA has an open
      // writer (and hence: open MP Upload)
      LOG.info("With Multipart Initiated In Write: Commit Job 1");
      commitJob(committer1, jContext1);
    }

    // job2/task writes its output to the destination and
    // closes the file
    writeOutput(recordWriter2, tContext2);

    // get the output file
    Path job2TaskOutputFile = recordWriter2.getDest();


    // commit the second task
    LOG.info("Commit Task 2");
    commitTask(committer2, tContext2);

    if (!multipartInitiatedInWrite) {
      // if not a magic committer, commit the job now. Because at
      // this point the staging committer tasks from job2 will be pending
      LOG.info("With Multipart NOT Initiated In Write: Commit Job 1");
      assertJobAttemptPathExists(committer1, jContext1);
      commitJob(committer1, jContext1);
    }

    // run the warning scan code, which will find output.
    // this can be manually reviewed in the logs to verify
    // readability
    committer2.warnOnActiveUploads(outDir);
    // and second job
    LOG.info("Commit Job 2");
    assertJobAttemptPathExists(committer2, jContext2);
    commitJob(committer2, jContext2);

    // validate the output
    Path job1Output = new Path(outDir, job1TaskOutputFile.getName());
    Path job2Output = new Path(outDir, job2TaskOutputFile.getName());
    assertNotEquals(job1Output, job2Output,
        "Job output file filenames must be different");

    // job1 output must be there
    assertPathExists("job 1 output", job1Output);
    // job 2 file is there
    assertPathExists("job 2 output", job2Output);

    // and nothing is pending
    assertNoMultipartUploadsPending(outDir);

  }

  /**
   * Verify self-generated UUID logic.
   * A committer used for job setup can also use it for task setup,
   * but a committer which generated a job ID but was only
   * used for task setup -that is rejected.
   * Task abort will still work.
   */
  @Test
  public void testSelfGeneratedUUID() throws Throwable {
    describe("Run two jobs to the same destination, assert they both complete");
    Configuration conf = getConfiguration();

    unsetUUIDOptions(conf);
    // job is set to generate UUIDs
    conf.setBoolean(FS_S3A_COMMITTER_GENERATE_UUID, true);

    // create the job. don't write anything
    JobData jobData = startJob(false);
    AbstractS3ACommitter committer = jobData.committer;
    String uuid = committer.getUUID();
    Assertions.assertThat(committer.getUUIDSource())
        .describedAs("UUID source of %s", committer)
        .isEqualTo(AbstractS3ACommitter.JobUUIDSource.GeneratedLocally);

    // examine the job configuration and verify that it has been updated
    Configuration jobConf = jobData.conf;
    Assertions.assertThat(jobConf.get(FS_S3A_COMMITTER_UUID, null))
        .describedAs("Config option " + FS_S3A_COMMITTER_UUID)
        .isEqualTo(uuid);
    Assertions.assertThat(jobConf.get(FS_S3A_COMMITTER_UUID_SOURCE, null))
        .describedAs("Config option " + FS_S3A_COMMITTER_UUID_SOURCE)
        .isEqualTo(AbstractS3ACommitter.JobUUIDSource.GeneratedLocally
            .getText());

    // because the task was set up in the job, it can have task
    // setup called, even though it had a random ID.
    committer.setupTask(jobData.tContext);

    // but a new committer will not be set up
    TaskAttemptContext tContext2 =
        new TaskAttemptContextImpl(conf, taskAttempt1);
    AbstractS3ACommitter committer2 = createCommitter(outDir, tContext2);
    Assertions.assertThat(committer2.getUUIDSource())
        .describedAs("UUID source of %s", committer2)
        .isEqualTo(AbstractS3ACommitter.JobUUIDSource.GeneratedLocally);
    assertNotEquals(committer.getUUID(),
        committer2.getUUID(), "job UUIDs");
    // Task setup MUST fail.
    intercept(PathCommitException.class,
        E_SELF_GENERATED_JOB_UUID, () -> {
        committer2.setupTask(tContext2);
        return committer2;
      });
    // task abort with the self-generated option is fine.
    committer2.abortTask(tContext2);
  }

  /**
   * Verify the option to require a UUID applies and
   * when a committer is instantiated without those options,
   * it fails early.
   */
  @Test
  public void testRequirePropagatedUUID() throws Throwable {
    Configuration conf = getConfiguration();

    unsetUUIDOptions(conf);
    conf.setBoolean(FS_S3A_COMMITTER_REQUIRE_UUID, true);
    conf.setBoolean(FS_S3A_COMMITTER_GENERATE_UUID, true);

    // create the job, expect a failure, even if UUID generation
    // is enabled.
    intercept(PathCommitException.class, E_NO_SPARK_UUID, () ->
        startJob(false));
  }

  /**
   * Strip staging/spark UUID options.
   * @param conf config
   * @return the patched config
   */
  protected Configuration unsetUUIDOptions(final Configuration conf) {
    conf.unset(SPARK_WRITE_UUID);
    conf.unset(FS_S3A_COMMITTER_UUID);
    conf.unset(FS_S3A_COMMITTER_GENERATE_UUID);
    conf.unset(FS_S3A_COMMITTER_REQUIRE_UUID);
    return conf;
  }

  /**
   * Assert that a committer's job attempt path exists.
   * For the staging committers, this is in the cluster FS.
   * @param committer committer
   * @param jobContext job context
   * @throws IOException failure
   */
  protected void assertJobAttemptPathExists(
      final AbstractS3ACommitter committer,
      final JobContext jobContext) throws IOException {
    Path attemptPath = committer.getJobAttemptPath(jobContext);
    ContractTestUtils.assertIsDirectory(
        attemptPath.getFileSystem(committer.getConf()),
        attemptPath);
  }

  @Test
  public void testS3ACommitterFactoryBinding() throws Throwable {
    describe("Verify that the committer factory returns this "
        + "committer when configured to do so");
    Job job = newJob();
    FileOutputFormat.setOutputPath(job, outDir);
    Configuration conf = job.getConfiguration();
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt0);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf,
        taskAttempt0);
    S3ACommitterFactory factory = new S3ACommitterFactory();
    Assertions.assertThat(factory.createOutputCommitter(outDir, tContext).getClass())
        .describedAs("Committer from factory with name %s", getCommitterName())
        .isEqualTo(createCommitter(outDir, tContext).getClass());
  }

  /**
   * Validate the path of a file being written to during the write
   * itself.
   * @param p path
   * @param expectedLength
   * @param jobId job id
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathDuringWrite(Path p,
      final long expectedLength,
      String jobId) throws IOException {

  }

  /**
   * Validate the path of a file being written to after the write
   * operation has completed.
   * @param p path
   * @param expectedLength
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptPathAfterWrite(Path p,
      final long expectedLength) throws IOException {

  }

  /**
   * Perform any actions needed to validate the working directory of
   * a committer.
   * For example: filesystem, path attributes
   * @param committer committer instance
   * @param context task attempt context
   * @throws IOException IO failure
   */
  protected void validateTaskAttemptWorkingDirectory(
      AbstractS3ACommitter committer,
      TaskAttemptContext context) throws IOException {
  }

  /**
   * Commit a task then validate the state of the committer afterwards.
   * @param committer committer
   * @param tContext task context
   * @throws IOException IO failure
   */
  protected void commitTask(final AbstractS3ACommitter committer,
      final TaskAttemptContext tContext) throws IOException {
    committer.commitTask(tContext);

  }

  /**
   * Commit a job then validate the state of the committer afterwards.
   * @param committer committer
   * @param jContext job context
   * @throws IOException IO failure
   */
  protected void commitJob(final AbstractS3ACommitter committer,
      final JobContext jContext) throws IOException {
    committer.commitJob(jContext);

  }

}
