/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Validate emulation of distributed cache load in gridmix simulated jobs.
 * 
 */
public class TestDistCacheEmulation {

  private DistributedCacheEmulator dce = null;

  @BeforeAll
  public static void init() throws IOException {
    GridmixTestUtils.initCluster(TestDistCacheEmulation.class);
    File target=new File("target"+File.separator+TestDistCacheEmulation.class.getName());
    if(!target.exists()){
      assertTrue(target.mkdirs());
    }
    
  }

  @AfterAll
  public static void shutDown() throws IOException {
    GridmixTestUtils.shutdownCluster();
  }

  /**
   * Validate the dist cache files generated by GenerateDistCacheData job.
   * 
   * @param jobConf
   *          configuration of GenerateDistCacheData job.
   * @param sortedFileSizes
   *          array of sorted distributed cache file sizes
   * @throws IOException
   * @throws FileNotFoundException
   */
  private void validateDistCacheData(Configuration jobConf,
      long[] sortedFileSizes) throws FileNotFoundException, IOException {
    Path distCachePath = dce.getDistributedCacheDir();
    String filesListFile = jobConf
        .get(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_LIST);
    FileSystem fs = FileSystem.get(jobConf);

    // Validate the existence of Distributed Cache files list file directly
    // under distributed cache directory
    Path listFile = new Path(filesListFile);
    assertTrue(distCachePath.equals(
        listFile.getParent().makeQualified(fs.getUri(), fs.getWorkingDirectory())),
        "Path of Distributed Cache files list file is wrong.");

    // Delete the dist cache files list file
    assertTrue(fs.delete(listFile, true),
        "Failed to delete distributed Cache files list file " + listFile);

    List<Long> fileSizes = new ArrayList<Long>();
    for (long size : sortedFileSizes) {
      fileSizes.add(size);
    }
    // validate dist cache files after deleting the 'files list file'
    validateDistCacheFiles(fileSizes, distCachePath);
  }

  /**
   * Validate private/public distributed cache files.
   * 
   * @param filesSizesExpected
   *          list of sizes of expected dist cache files
   * @param distCacheDir
   *          the distributed cache dir to be validated
   * @throws IOException
   * @throws FileNotFoundException
   */
  private void validateDistCacheFiles(List<Long> filesSizesExpected, Path distCacheDir)
      throws FileNotFoundException, IOException {
    // RemoteIterator<LocatedFileStatus> iter =
    FileStatus[] statuses = GridmixTestUtils.dfs.listStatus(distCacheDir);
    int numFiles = filesSizesExpected.size();
    assertEquals(numFiles, statuses.length,
        "Number of files under distributed cache dir is wrong.");
    for (int i = 0; i < numFiles; i++) {
      FileStatus stat = statuses[i];
      assertTrue(filesSizesExpected.remove(stat.getLen()), "File size of distributed cache file "
          + stat.getPath().toUri().getPath() + " is wrong.");

      FsPermission perm = stat.getPermission();
      assertEquals(new FsPermission(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_PERM), perm,
          "Wrong permissions for distributed cache file "
          + stat.getPath().toUri().getPath());
    }
  }

  /**
   * Configures 5 HDFS-based dist cache files and 1 local-FS-based dist cache
   * file in the given Configuration object <code>conf</code>.
   * 
   * @param conf
   *          configuration where dist cache config properties are to be set
   * @return array of sorted HDFS-based distributed cache file sizes
   * @throws IOException
   */
  private long[] configureDummyDistCacheFiles(Configuration conf)
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set("user.name", user);
    
    // Set some dummy dist cache files in gridmix configuration so that they go
    // into the configuration of JobStory objects.
    String[] distCacheFiles = { "hdfs:///tmp/file1.txt",
        "/tmp/" + user + "/.staging/job_1/file2.txt",
        "hdfs:///user/user1/file3.txt", "/home/user2/file4.txt",
        "subdir1/file5.txt", "subdir2/file6.gz" };

    String[] fileSizes = { "400", "2500", "700", "1200", "1500", "500" };

    String[] visibilities = { "true", "false", "false", "true", "true", "false" };
    String[] timeStamps = { "1234", "2345", "34567", "5434", "125", "134" };

    // DistributedCache.setCacheFiles(fileCaches, conf);
    conf.setStrings(MRJobConfig.CACHE_FILES, distCacheFiles);
    conf.setStrings(MRJobConfig.CACHE_FILES_SIZES, fileSizes);
    conf.setStrings(JobContext.CACHE_FILE_VISIBILITIES, visibilities);
    conf.setStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS, timeStamps);

    // local FS based dist cache file whose path contains <user>/.staging is
    // not created on HDFS. So file size 2500 is not added to sortedFileSizes.
    long[] sortedFileSizes = new long[] { 1500, 1200, 700, 500, 400 };
    return sortedFileSizes;
  }

  /**
   * Runs setupGenerateDistCacheData() on a new DistrbutedCacheEmulator and and
   * returns the jobConf. Fills the array <code>sortedFileSizes</code> that can
   * be used for validation. Validation of exit code from
   * setupGenerateDistCacheData() is done.
   * 
   * @param generate
   *          true if -generate option is specified
   * @param sortedFileSizes
   *          sorted HDFS-based distributed cache file sizes
   * @throws IOException
   * @throws InterruptedException
   */
  private Configuration runSetupGenerateDistCacheData(boolean generate,
      long[] sortedFileSizes) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    long[] fileSizes = configureDummyDistCacheFiles(conf);
    System.arraycopy(fileSizes, 0, sortedFileSizes, 0, fileSizes.length);

    // Job stories of all 3 jobs will have same dist cache files in their
    // configurations
    final int numJobs = 3;
    DebugJobProducer jobProducer = new DebugJobProducer(numJobs, conf);

    Configuration jobConf = GridmixTestUtils.mrvl.getConfig();
    Path ioPath = new Path("testSetupGenerateDistCacheData")
        .makeQualified(GridmixTestUtils.dfs.getUri(),GridmixTestUtils.dfs.getWorkingDirectory());
    FileSystem fs = FileSystem.get(jobConf);
    if (fs.exists(ioPath)) {
      fs.delete(ioPath, true);
    }
    FileSystem.mkdirs(fs, ioPath, new FsPermission((short) 0777));

    dce = createDistributedCacheEmulator(jobConf, ioPath, generate);
    int exitCode = dce.setupGenerateDistCacheData(jobProducer);
    int expectedExitCode = generate ? 0
        : Gridmix.MISSING_DIST_CACHE_FILES_ERROR;
    assertEquals(expectedExitCode, exitCode, "setupGenerateDistCacheData failed.");

    // reset back
    resetDistCacheConfigProperties(jobConf);
    return jobConf;
  }

  /**
   * Reset the config properties related to Distributed Cache in the given job
   * configuration <code>jobConf</code>.
   * 
   * @param jobConf
   *          job configuration
   */
  private void resetDistCacheConfigProperties(Configuration jobConf) {
    // reset current/latest property names
    jobConf.setStrings(MRJobConfig.CACHE_FILES, "");
    jobConf.setStrings(MRJobConfig.CACHE_FILES_SIZES, "");
    jobConf.setStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS, "");
    jobConf.setStrings(JobContext.CACHE_FILE_VISIBILITIES, "");
    // reset old property names
    jobConf.setStrings("mapred.cache.files", "");
    jobConf.setStrings("mapred.cache.files.filesizes", "");
    jobConf.setStrings("mapred.cache.files.visibilities", "");
    jobConf.setStrings("mapred.cache.files.timestamps", "");
  }

  /**
   * Validate GenerateDistCacheData job if it creates dist cache files properly.
   * 
   * @throws Exception
   */
  @Test
  @Timeout(value = 200)
  public void testGenerateDistCacheData() throws Exception {
    long[] sortedFileSizes = new long[5];
    Configuration jobConf = runSetupGenerateDistCacheData(true, sortedFileSizes);
    GridmixJob gridmixJob = new GenerateDistCacheData(jobConf);
    Job job = gridmixJob.call();
    assertEquals(0, job.getNumReduceTasks(),
        "Number of reduce tasks in GenerateDistCacheData is not 0.");
    assertTrue(job.waitForCompletion(false), "GenerateDistCacheData job failed.");
    validateDistCacheData(jobConf, sortedFileSizes);
  }

  /**
   * Validate setupGenerateDistCacheData by validating <li>permissions of the
   * distributed cache directories and <li>content of the generated sequence
   * file. This includes validation of dist cache file paths and their file
   * sizes.
   */
  private void validateSetupGenDC(Configuration jobConf, long[] sortedFileSizes)
      throws IOException, InterruptedException {
    // build things needed for validation
    long sumOfFileSizes = 0;
    for (int i = 0; i < sortedFileSizes.length; i++) {
      sumOfFileSizes += sortedFileSizes[i];
    }

    FileSystem fs = FileSystem.get(jobConf);
    assertEquals(sortedFileSizes.length,
        jobConf.getInt(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_COUNT, -1),
        "Number of distributed cache files to be generated is wrong.");
    assertEquals(sumOfFileSizes,
        jobConf.getLong(GenerateDistCacheData.GRIDMIX_DISTCACHE_BYTE_COUNT, -1),
        "Total size of dist cache files to be generated is wrong.");
    Path filesListFile = new Path(
        jobConf.get(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_LIST));
    FileStatus stat = fs.getFileStatus(filesListFile);
    assertEquals(new FsPermission((short) 0644), stat.getPermission(),
        "Wrong permissions of dist Cache files list file "
        + filesListFile);

    InputSplit split = new FileSplit(filesListFile, 0, stat.getLen(),
        (String[]) null);
    TaskAttemptContext taskContext = MapReduceTestUtil
        .createDummyMapTaskAttemptContext(jobConf);
    RecordReader<LongWritable, BytesWritable> reader = new GenerateDistCacheData.GenDCDataFormat()
        .createRecordReader(split, taskContext);
    MapContext<LongWritable, BytesWritable, NullWritable, BytesWritable> mapContext = new MapContextImpl<LongWritable, BytesWritable, NullWritable, BytesWritable>(
        jobConf, taskContext.getTaskAttemptID(), reader, null, null,
        MapReduceTestUtil.createDummyReporter(), split);
    reader.initialize(split, mapContext);

    // start validating setupGenerateDistCacheData
    doValidateSetupGenDC(reader, fs, sortedFileSizes);
  }

  /**
   * Validate setupGenerateDistCacheData by validating <li>permissions of the
   * distributed cache directory and <li>content of the generated sequence file.
   * This includes validation of dist cache file paths and their file sizes.
   */
  private void doValidateSetupGenDC(
      RecordReader<LongWritable, BytesWritable> reader, FileSystem fs,
      long[] sortedFileSizes) throws IOException, InterruptedException {

    // Validate permissions of dist cache directory
    Path distCacheDir = dce.getDistributedCacheDir();
    assertEquals(fs.getFileStatus(distCacheDir).getPermission().getOtherAction()
        .and(FsAction.EXECUTE), FsAction.EXECUTE,
        "Wrong permissions for distributed cache dir " + distCacheDir);

    // Validate the content of the sequence file generated by
    // dce.setupGenerateDistCacheData().
    LongWritable key = new LongWritable();
    BytesWritable val = new BytesWritable();
    for (int i = 0; i < sortedFileSizes.length; i++) {
      assertTrue(reader.nextKeyValue(),
          "Number of files written to the sequence file by "
          + "setupGenerateDistCacheData is less than the expected.");
      key = reader.getCurrentKey();
      val = reader.getCurrentValue();
      long fileSize = key.get();
      String file = new String(val.getBytes(), 0, val.getLength());

      // Dist Cache files should be sorted based on file size.
      assertEquals(sortedFileSizes[i], fileSize, "Dist cache file size is wrong.");

      // Validate dist cache file path.

      // parent dir of dist cache file
      Path parent = new Path(file).getParent().makeQualified(fs.getUri(),fs.getWorkingDirectory());
      // should exist in dist cache dir
      assertTrue(distCacheDir.equals(parent),
          "Public dist cache file path is wrong.");
    }
  }

  /**
   * Test if DistributedCacheEmulator's setup of GenerateDistCacheData is
   * working as expected.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  @Timeout(value = 20)
  public void testSetupGenerateDistCacheData() throws IOException,
      InterruptedException {
    long[] sortedFileSizes = new long[5];
    Configuration jobConf = runSetupGenerateDistCacheData(true, sortedFileSizes);
    validateSetupGenDC(jobConf, sortedFileSizes);

    // Verify if correct exit code is seen when -generate option is missing and
    // distributed cache files are missing in the expected path.
    runSetupGenerateDistCacheData(false, sortedFileSizes);
  }

  /**
   * Create DistributedCacheEmulator object and do the initialization by calling
   * init() on it with dummy trace. Also configure the pseudo local FS.
   */
  private DistributedCacheEmulator createDistributedCacheEmulator(
      Configuration conf, Path ioPath, boolean generate) throws IOException {
    DistributedCacheEmulator dce = new DistributedCacheEmulator(conf, ioPath);
    JobCreator jobCreator = JobCreator.getPolicy(conf, JobCreator.LOADJOB);
    jobCreator.setDistCacheEmulator(dce);
    dce.init("dummytrace", jobCreator, generate);
    return dce;
  }

  /**
   * Test the configuration property for disabling/enabling emulation of
   * distributed cache load.
   */
  @Test
  @Timeout(value = 2)
  public void testDistCacheEmulationConfigurability() throws IOException {
    Configuration jobConf = GridmixTestUtils.mrvl.getConfig();
    Path ioPath = new Path("testDistCacheEmulationConfigurability")
        .makeQualified(GridmixTestUtils.dfs.getUri(),GridmixTestUtils.dfs.getWorkingDirectory());
    FileSystem fs = FileSystem.get(jobConf);
    FileSystem.mkdirs(fs, ioPath, new FsPermission((short) 0777));

    // default config
    dce = createDistributedCacheEmulator(jobConf, ioPath, false);
    assertTrue(dce.shouldEmulateDistCacheLoad(), "Default configuration of "
        + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
        + " is wrong.");

    // config property set to false
    jobConf.setBoolean(
        DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE, false);
    dce = createDistributedCacheEmulator(jobConf, ioPath, false);
    assertFalse(dce.shouldEmulateDistCacheLoad(),
        "Disabling of emulation of distributed cache load by setting "
        + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
        + " to false is not working.");
  }
/** 
 * test method configureDistCacheFiles
 * 
 */
  @Test
  @Timeout(value = 2)
  public void testDistCacheEmulator() throws Exception {

    Configuration conf = new Configuration();
    configureDummyDistCacheFiles(conf);
    File ws = new File("target" + File.separator + this.getClass().getName());
    Path ioPath = new Path(ws.getAbsolutePath());

    DistributedCacheEmulator dce = new DistributedCacheEmulator(conf, ioPath);
    JobConf jobConf = new JobConf(conf);
    jobConf.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    File fin=new File("src"+File.separator+"test"+File.separator+"resources"+File.separator+"data"+File.separator+"wordcount.json");
    dce.init(fin.getAbsolutePath(), JobCreator.LOADJOB, true);
    dce.configureDistCacheFiles(conf, jobConf);
    
    String[] caches=conf.getStrings(MRJobConfig.CACHE_FILES);
    String[] tmpfiles=conf.getStrings("tmpfiles");
    // this method should fill caches AND tmpfiles  from MRJobConfig.CACHE_FILES property 
    assertEquals(6, ((caches==null?0:caches.length)+(tmpfiles==null?0:tmpfiles.length)));
  }
}
