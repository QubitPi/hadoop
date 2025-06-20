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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitUtilsWithMR;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.TASK_ATTEMPT_ID;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.TEMP_DATA;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.impl.CommitUtilsWithMR.*;
import static org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTrackerUtils.isCleanupMagicCommitterEnabled;
import static org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTrackerUtils.isTrackMagicCommitsInMemoryEnabled;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;

/**
 * This is a dedicated committer which requires the "magic" directory feature
 * of the S3A Filesystem to be enabled; it then uses paths for task and job
 * attempts in magic paths, so as to ensure that the final output goes direct
 * to the destination directory.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MagicS3GuardCommitter extends AbstractS3ACommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(MagicS3GuardCommitter.class);

  /** Name: {@value}. */
  public static final String NAME = CommitConstants.COMMITTER_NAME_MAGIC;

  /**
   * Create a task committer.
   * @param outputPath the job's output path
   * @param context the task's context
   * @throws IOException on a failure
   */
  public MagicS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    setWorkPath(getTaskAttemptPath(context));
    verifyIsMagicCommitPath(getDestS3AFS(), getWorkPath());
    LOG.debug("Task attempt {} has work path {}",
        context.getTaskAttemptID(),
        getWorkPath());
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Require magic paths in the FS client.
   * @return true, always.
   */
  @Override
  protected boolean requiresDelayedCommitOutputInFileSystem() {
    return true;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Setup Job %s", jobIdString(context))) {
      super.setupJob(context);
      Path jobPath = getJobPath();
      final FileSystem destFS = getDestinationFS(jobPath,
          context.getConfiguration());
      destFS.mkdirs(jobPath);
    }
  }

  /**
   * Get the list of pending uploads for this job attempt, by listing
   * all .pendingset files in the job attempt directory.
   * @param commitContext job context
   * @return a list of pending commits.
   * @throws IOException Any IO failure
   */
  protected ActiveCommit listPendingUploadsToCommit(
      CommitContext commitContext)
      throws IOException {
    FileSystem fs = getDestFS();
    return ActiveCommit.fromStatusIterator(fs,
        listAndFilter(fs, getJobAttemptPath(commitContext.getJobContext()),
            false,
            CommitOperations.PENDINGSET_FILTER));
  }

  /**
   * Delete the magic directory.
   */
  public void cleanupStagingDirs() {
    if (isCleanupMagicCommitterEnabled(getConf())) {
      final Path out = getOutputPath();
      Path path = getMagicJobPath(getUUID(), out);
      try(DurationInfo ignored = new DurationInfo(LOG, true,
              "Deleting magic directory %s", path)) {
        Invoker.ignoreIOExceptions(LOG, "cleanup magic directory", path.toString(),
                () -> deleteWithWarning(getDestFS(), path, true));
        // and the job temp directory with manifests
        Invoker.ignoreIOExceptions(LOG, "cleanup job directory", path.toString(),
                () -> deleteWithWarning(getDestFS(),
                        new Path(out, TEMP_DATA), true));
      }
    }
  }

  /**
   * Did this task write any files in the work directory?
   * Probes for a task existing by looking to see if the attempt dir exists.
   * This adds more HTTP requests to the call. It may be better just to
   * return true and rely on the commit task doing the work.
   * @param context the task's context
   * @return true if the attempt path exists
   * @throws IOException failure to list the path
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    // return true as a dir was created here in setup;
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Commit task %s", context.getTaskAttemptID())) {
      PendingSet commits = innerCommitTask(context);
      LOG.info("Task {} committed {} files", context.getTaskAttemptID(),
          commits.size());
    } catch (IOException e) {
      getCommitOperations().taskCompleted(false);
      throw e;
    } finally {
      // delete the task attempt so there's no possibility of a second attempt
      // incurs a LIST, a bulk DELETE and maybe a parent dir creation, however
      // as it happens during task commit, it should be off the critical path.
      deleteTaskAttemptPathQuietly(context);
    }
    getCommitOperations().taskCompleted(true);
    LOG.debug("aggregate statistics\n{}",
        demandStringifyIOStatistics(getIOStatistics()));
  }

  /**
   * Inner routine for committing a task.
   * The list of pending commits is loaded and then saved to the job attempt
   * dir in a single pendingset file.
   * Failure to load any file or save the final file triggers an abort of
   * all known pending commits.
   * @param context context
   * @return the summary file
   * @throws IOException exception
   */
  private PendingSet innerCommitTask(
      TaskAttemptContext context) throws IOException {
    // load in all pending commits.
    PendingSet pendingSet = loadPendingCommits(context);
    try (CommitContext commitContext = initiateTaskOperation(context)) {
      // patch in IDs
      String jobId = getUUID();
      String taskId = String.valueOf(context.getTaskAttemptID());
      for (SinglePendingCommit commit : pendingSet.getCommits()) {
        commit.setJobId(jobId);
        commit.setTaskId(taskId);
      }
      pendingSet.putExtraData(TASK_ATTEMPT_ID, taskId);
      pendingSet.setJobId(jobId);
      // add in the IOStatistics of all the file loading
      if (commitContext.isCollectIOStatistics()) {
        pendingSet.getIOStatistics()
            .aggregate(
                commitContext.getIOStatisticsContext().getIOStatistics());
      }

      Path jobAttemptPath = getJobAttemptPath(context);
      TaskAttemptID taskAttemptID = context.getTaskAttemptID();
      Path taskOutcomePath = new Path(jobAttemptPath,
          taskAttemptID.getTaskID().toString() +
              CommitConstants.PENDINGSET_SUFFIX);
      LOG.info("Saving work of {} to {}", taskAttemptID, taskOutcomePath);
      LOG.debug("task statistics\n{}",
          IOStatisticsLogging.demandStringifyIOStatisticsSource(pendingSet));
      try {
        // We will overwrite if there exists a pendingSet file already
        pendingSet.save(getDestFS(),
            taskOutcomePath,
            commitContext.getPendingSetSerializer());
      } catch (IOException e) {
        LOG.warn("Failed to save task commit data to {} ",
            taskOutcomePath, e);
        abortPendingUploads(commitContext, pendingSet.getCommits(), true);
        throw e;
      }
    }
    return pendingSet;
  }

  /**
   * Loads pending commits from either memory or from the remote store (S3) based on the config.
   * @param context TaskAttemptContext
   * @return All pending commit data for the given TaskAttemptContext
   * @throws IOException
   *           if there is an error trying to read the commit data
   */
  protected PendingSet loadPendingCommits(TaskAttemptContext context) throws IOException {
    PendingSet pendingSet = new PendingSet();
    if (isTrackMagicCommitsInMemoryEnabled(context.getConfiguration())) {
      // load from memory
      List<SinglePendingCommit> pendingCommits = loadPendingCommitsFromMemory(context);

      for (SinglePendingCommit singleCommit : pendingCommits) {
        // aggregate stats
        pendingSet.getIOStatistics()
            .aggregate(singleCommit.getIOStatistics());
        // then clear so they aren't marshalled again.
        singleCommit.getIOStatistics().clear();
      }
      pendingSet.setCommits(pendingCommits);
    } else {
      // Load from remote store
      CommitOperations actions = getCommitOperations();
      Path taskAttemptPath = getTaskAttemptPath(context);
      try (CommitContext commitContext = initiateTaskOperation(context)) {
        Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>> loaded =
            actions.loadSinglePendingCommits(taskAttemptPath, true, commitContext);
        pendingSet = loaded.getKey();
        List<Pair<LocatedFileStatus, IOException>> failures = loaded.getValue();
        if (!failures.isEmpty()) {
          // At least one file failed to load
          // revert all which did; report failure with first exception
          LOG.error("At least one commit file could not be read: failing");
          abortPendingUploads(commitContext, pendingSet.getCommits(), true);
          throw failures.get(0).getValue();
        }
      }
    }
    return pendingSet;
  }

  /**
   * Loads the pending commits from the memory data structure for a given taskAttemptId.
   * @param context TaskContext
   * @return list of pending commits
   */
  private List<SinglePendingCommit> loadPendingCommitsFromMemory(TaskAttemptContext context) {
    String taskAttemptId = String.valueOf(context.getTaskAttemptID());
    // get all the pending commit metadata associated with the taskAttemptId.
    // This will also remove the entry from the map.
    List<SinglePendingCommit> pendingCommits =
        InMemoryMagicCommitTracker.getTaskAttemptIdToMpuMetadata().remove(taskAttemptId);
    // get all the path/files associated with the taskAttemptId.
    // This will also remove the entry from the map.
    List<Path> pathsAssociatedWithTaskAttemptId =
        InMemoryMagicCommitTracker.getTaskAttemptIdToPath().remove(taskAttemptId);

    // for each of the path remove the entry from map,
    // This is done so that there is no memory leak.
    if (pathsAssociatedWithTaskAttemptId != null) {
      for (Path path : pathsAssociatedWithTaskAttemptId) {
        boolean cleared =
            InMemoryMagicCommitTracker.getPathToBytesWritten().remove(path) != null;
        LOG.debug("Removing path: {} from the memory isSuccess: {}", path, cleared);
      }
    } else {
      LOG.debug("No paths to remove for taskAttemptId: {}", taskAttemptId);
    }

    if (pendingCommits == null || pendingCommits.isEmpty()) {
      LOG.info("No commit data present for the taskAttemptId: {} in the memory", taskAttemptId);
      return new ArrayList<>();
    }

    return pendingCommits;
  }

  /**
   * Abort a task. Attempt load then abort all pending files,
   * then try to delete the task attempt path.
   * This method may be called on the job committer, rather than the
   * task one (such as in the MapReduce AM after a task container failure).
   * It must extract all paths and state from the passed in context.
   * @param context task context
   * @throws IOException if there was some problem querying the path other
   * than it not actually existing.
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "Abort task %s", context.getTaskAttemptID());
        CommitContext commitContext = initiateTaskOperation(context)) {
      if (isTrackMagicCommitsInMemoryEnabled(context.getConfiguration())) {
        List<SinglePendingCommit> pendingCommits = loadPendingCommitsFromMemory(context);
        for (SinglePendingCommit singleCommit : pendingCommits) {
          commitContext.abortSingleCommit(singleCommit);
        }
      } else {
        getCommitOperations().abortAllSinglePendingCommits(attemptPath, commitContext, true);
      }
    } finally {
      deleteQuietly(
          attemptPath.getFileSystem(context.getConfiguration()),
          attemptPath, true);
    }
  }

  /**
   * Compute the path under which all job attempts will be placed.
   * @return the path to store job attempt data.
   */
  @Override
  protected Path getJobPath() {
    return getMagicJobPath(getUUID(), getOutputPath());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * For the magic committer, the path includes the job UUID.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected final Path getJobAttemptPath(int appAttemptId) {
    return getMagicJobAttemptPath(getUUID(), appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public final Path getTaskAttemptPath(TaskAttemptContext context) {
    return getMagicTaskAttemptPath(context, getUUID(), getOutputPath());
  }

  @Override
  protected final Path getBaseTaskAttemptPath(TaskAttemptContext context) {
    return getBaseMagicTaskAttemptPath(context, getUUID(), getOutputPath());
  }

  /**
   * Get a temporary directory for data. When a task is aborted/cleaned
   * up, the contents of this directory are all deleted.
   * @param context task context
   * @return a path for temporary data.
   */
  public Path getTempTaskAttemptPath(TaskAttemptContext context) {
    return CommitUtilsWithMR.getTempTaskAttemptPath(context,
        getUUID(),
        getOutputPath());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MagicCommitter{");
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }
}
