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

package org.apache.hadoop.fs.s3a.commit.terasort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraSortConfigKeys;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.commit.AbstractYarnClusterITest;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static java.util.Optional.empty;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs Terasort against S3A.
 *
 * Parameterized by committer name, using a YARN cluster
 * shared across all test runs.
 * The tests run in sequence, so each operation is isolated.
 * This also means that the test paths are deleted in test
 * teardown; shared variables must all be static.
 *
 * The test is a scale test; for each parameter it takes a few minutes to
 * run the full suite.
 * Before anyone calls that out as slow: try running the test with the file
 * committer.
 */
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
@SuppressWarnings("StaticNonFinalField")
public class ITestTerasortOnS3A extends AbstractYarnClusterITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestTerasortOnS3A.class);

  public static final int EXPECTED_PARTITION_COUNT = 10;

  public static final int PARTITION_SAMPLE_SIZE = 1000;

  public static final int ROW_COUNT = 1000;

  /**
   * Duration tracker created in the first of the test cases and closed
   * in {@link #test_140_teracomplete(String, boolean)}.
   */
  private static Optional<DurationInfo> terasortDuration = empty();

  /**
   * Tracker of which stages are completed and how long they took.
   */
  private static Map<String, DurationInfo> completedStages = new HashMap<>();

  /** Name of the committer for this run. */
  private String committerName;

  /** Should Magic committer track pending commits in-memory. */
  private boolean trackCommitsInMemory;

  /** Base path for all the terasort input and output paths. */
  private Path terasortPath;

  /** Input (teragen) path. */
  private Path sortInput;

  /** Path where sorted data goes. */
  private Path sortOutput;

  /** Path for validated job's output. */
  private Path sortValidate;

  /**
   * Test array for parameterized test runs.
   *
   * @return the committer binding for this run.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {DirectoryStagingCommitter.NAME, false},
        {MagicS3GuardCommitter.NAME, false},
        {MagicS3GuardCommitter.NAME, true}});
  }

  public void initITestTerasortOnS3A(
      final String pCommitterName, final boolean pTrackCommitsInMemory) throws Exception {
    this.committerName = pCommitterName;
    this.trackCommitsInMemory = pTrackCommitsInMemory;
  }

  @Override
  protected String committerName() {
    return committerName;
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    requireScaleTestsEnabled();
    prepareToTerasort();
  }

  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    /* no-op */
  }

  /**
   * Set up the job conf with the options for terasort chosen by the scale
   * options.
   * @param conf configuration
   */
  @Override
  protected void applyCustomConfigOptions(JobConf conf) {
    // small sample size for faster runs
    conf.setInt(TeraSortConfigKeys.SAMPLE_SIZE.key(),
        getSampleSizeForEachPartition());
    conf.setInt(TeraSortConfigKeys.NUM_PARTITIONS.key(),
        getExpectedPartitionCount());
    conf.setBoolean(
        TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(),
        false);
    conf.setBoolean(
        CommitConstants.FS_S3A_COMMITTER_MAGIC_TRACK_COMMITS_IN_MEMORY_ENABLED,
        trackCommitsInMemory);
  }

  private int getExpectedPartitionCount() {
    return EXPECTED_PARTITION_COUNT;
  }

  private int getSampleSizeForEachPartition() {
    return PARTITION_SAMPLE_SIZE;
  }

  protected int getRowCount() {
    return ROW_COUNT;
  }

  /**
   * Set up the terasort by initializing paths variables
   * The paths used must be unique across parameterized runs but
   * common across all test cases in a single parameterized run.
   */
  private void prepareToTerasort() throws IOException {
    // small sample size for faster runs
    terasortPath = getFileSystem().qualify(
        new Path(S3ATestUtils.createTestPath(new Path("terasort-test")),
            "terasort-" + committerName + "-" + trackCommitsInMemory));
    sortInput = new Path(terasortPath, "sortin");
    sortOutput = new Path(terasortPath, "sortout");
    sortValidate = new Path(terasortPath, "validate");
  }

  /**
   * Declare that a stage has completed.
   * @param stage stage name/key in the map
   * @param d duration.
   */
  private static void completedStage(final String stage,
      final DurationInfo d) {
    completedStages.put(stage, d);
  }

  /**
   * Declare a stage which is required for this test case.
   * @param stage stage name
   */
  private static void requireStage(final String stage) {
    assumeTrue(completedStages.get(stage) != null,
        "Required stage was not completed: " + stage);
  }

  /**
   * Execute a single stage in the terasort.
   * Updates the completed stages map with the stage duration -if successful.
   * @param stage Stage name for the stages map.
   * @param jobConf job conf
   * @param dest destination directory -the _SUCCESS file will be expected here.
   * @param tool tool to run.
   * @param args args for the tool.
   * @param minimumFileCount minimum number of files to have been created
   * @throws Exception any failure
   */
  private void executeStage(
      final String stage,
      final JobConf jobConf,
      final Path dest,
      final Tool tool,
      final String[] args,
      final int minimumFileCount) throws Exception {
    int result;
    DurationInfo d = new DurationInfo(LOG, stage);
    try {
      result = ToolRunner.run(jobConf, tool, args);
    } finally {
      d.close();
    }
    dumpOutputTree(dest);
    assertEquals(0, result, stage
        + "(" + StringUtils.join(", ", args) + ")"
        + " failed");
    validateSuccessFile(dest, committerName(), getFileSystem(), stage,
        minimumFileCount, "");
    completedStage(stage, d);
  }

  /**
   * Set up terasort by cleaning out the destination, and note the initial
   * time before any of the jobs are executed.
   *
   * This is executed first <i>for each parameterized run</i>.
   * It is where all variables which need to be reset for each run need
   * to be reset.
   */
  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_100_terasort_setup(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    describe("Setting up for a terasort with path of %s", terasortPath);

    getFileSystem().delete(terasortPath, true);
    completedStages = new HashMap<>();
    terasortDuration = Optional.of(new DurationInfo(LOG, false, "Terasort"));
  }

  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_110_teragen(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    describe("Teragen to %s", sortInput);
    getFileSystem().delete(sortInput, true);

    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("teragen",
        jobConf,
        sortInput,
        new TeraGen(),
        new String[]{Integer.toString(getRowCount()), sortInput.toString()},
        1);
  }


  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_120_terasort(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    describe("Terasort from %s to %s", sortInput, sortOutput);
    requireStage("teragen");
    getFileSystem().delete(sortOutput, true);

    loadSuccessFile(getFileSystem(), sortInput, "previous teragen stage");
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("terasort",
        jobConf,
        sortOutput,
        new TeraSort(),
        new String[]{sortInput.toString(), sortOutput.toString()},
        1);
  }

  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_130_teravalidate(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    describe("TeraValidate from %s to %s", sortOutput, sortValidate);
    requireStage("terasort");
    getFileSystem().delete(sortValidate, true);
    loadSuccessFile(getFileSystem(), sortOutput, "previous terasort stage");
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("teravalidate",
        jobConf,
        sortValidate,
        new TeraValidate(),
        new String[]{sortOutput.toString(), sortValidate.toString()},
        1);
  }

  /**
   * Print the results, and save to the base dir as a CSV file.
   * Why there? Makes it easy to list and compare.
   */
  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_140_teracomplete(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    terasortDuration.ifPresent(d -> {
      d.close();
      completedStage("overall", d);
    });

    final StringBuilder results = new StringBuilder();
    results.append("\"Operation\"\t\"Duration\"\n");

    // this is how you dynamically create a function in a method
    // for use afterwards.
    // Works because there's no IOEs being raised in this sequence.
    Consumer<String> stage = (s) -> {
      DurationInfo duration = completedStages.get(s);
      results.append(String.format("\"%s\"\t\"%s\"\n",
          s,
          duration == null ? "" : duration));
    };

    stage.accept("teragen");
    stage.accept("terasort");
    stage.accept("teravalidate");
    stage.accept("overall");
    String text = results.toString();
    File resultsFile = new File(getReportDir(),
        String.format("%s-%s.csv", committerName, trackCommitsInMemory));
    FileUtils.write(resultsFile, text, StandardCharsets.UTF_8);
    LOG.info("Results are in {}\n{}", resultsFile, text);
  }

  /**
   * Reset the duration so if two committer tests are run sequentially.
   * Without this the total execution time is reported as from the start of
   * the first test suite to the end of the second.
   */
  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_150_teracleanup(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    terasortDuration = Optional.empty();
  }

  @MethodSource("params")
  @ParameterizedTest(name = "{0}-memory={1}")
  public void test_200_directory_deletion(String pCommitterName,
      boolean pTrackCommitsInMemory) throws Throwable {
    initITestTerasortOnS3A(pCommitterName, pTrackCommitsInMemory);
    getFileSystem().delete(terasortPath, true);
  }

  /**
   * Dump the files under a path -but not fail if the path is not present.,
   * @param path path to dump
   * @throws Exception any failure.
   */
  protected void dumpOutputTree(Path path) throws Exception {
    LOG.info("Files under output directory {}", path);
    try {
      lsR(getFileSystem(), path, true);
    } catch (FileNotFoundException e) {
      LOG.info("Output directory {} not found", path);
    }
  }
}
