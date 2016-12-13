/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.data;

import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * This class represents information contained in a job runtime process.
 */
public class SparkJobProgressData {
  private static final Logger logger = Logger.getLogger(SparkJobProgressData.class);
  private final Map<Integer, JobInfo> _jobIdToInfo = new HashMap<Integer, JobInfo>();
  private final Set<Integer> _completedJobs = new HashSet<Integer>();
  private final Set<Integer> _failedJobs = new HashSet<Integer>();

  private final Map<StageAttemptId, StageInfo> _stageIdToInfo = new HashMap<StageAttemptId, StageInfo>();
  private final Set<StageAttemptId> _completedStages = new HashSet<StageAttemptId>();
  private final Set<StageAttemptId> _failedStages = new HashSet<StageAttemptId>();

  // InputBytes => IB, OutputBytes => OB, ShuffleReadBytes => SRB, ShuffleWriteBytes => SWB
  private final List<String> _dataskewSchema =
          new ArrayList<String>(Arrays.asList("Input", "Output", "ShuffleRead", "ShuffleWrite"));

  public void addJobInfo(int jobId, JobInfo info) {
    _jobIdToInfo.put(jobId, info);
  }

  public void addCompletedJob(int jobId) {
    _completedJobs.add(jobId);
  }

  public void addFailedJob(int jobId) {
    _failedJobs.add(jobId);
  }

  public void addStageInfo(int stageId, int attemptId, StageInfo info) {
    _stageIdToInfo.put(new StageAttemptId(stageId, attemptId), info);
  }

  public void addCompletedStages(int stageId, int attemptId) {
    _completedStages.add(new StageAttemptId(stageId, attemptId));
  }

  public void addFailedStages(int stageId, int attemptId) {
    _failedStages.add(new StageAttemptId(stageId, attemptId));
  }

  public Set<Integer> getCompletedJobs() {
    return _completedJobs;
  }

  public Set<Integer> getFailedJobs() {
    return _failedJobs;
  }

  private static double getFailureRate(int numCompleted, int numFailed) {
    int num = numCompleted + numFailed;

    if (num == 0) {
      return 0d;
    }

    return numFailed * 1.0d / num;
  }

  public double getJobFailureRate() {
    return getFailureRate(_completedJobs.size(), _failedJobs.size());
  }

  public double getStageFailureRate() {
    return getFailureRate(_completedStages.size(), _failedStages.size());
  }

  public JobInfo getJobInfo(int jobId) {
    return _jobIdToInfo.get(jobId);
  }

  public Map<StageAttemptId, StageInfo> getStageInfo() {
    return _stageIdToInfo;
  }

  public StageInfo getStageInfo(int stageId, int attemptId) {
    return _stageIdToInfo.get(new StageAttemptId(stageId, attemptId));
  }

  public Set<StageAttemptId> getCompletedStages() {
    return _completedStages;
  }

  public Set<StageAttemptId> getFailedStages() {
    return _failedStages;
  }

  public List<String> getDataSkewSchema() {
    return _dataskewSchema;
  }

  /**
   * Job itself does not have a name, it will use its latest stage as the name.
   *
   * @param jobId
   * @return
   */
  public String getJobDescription(int jobId) {
    List<Integer> stageIds = _jobIdToInfo.get(jobId).stageIds;
    int id = -1;
    for (int stageId : stageIds) {
      id = Math.max(id, stageId);
    }
    if (id == -1) {
      logger.error("Spark Job id [" + jobId + "] does not contain any stage.");
      return null;
    }
    return _stageIdToInfo.get(new StageAttemptId(id, 0)).name;
  }

  public List<String> getFailedJobDescriptions() {
    List<String> result = new ArrayList<String>();
    for (int id : _failedJobs) {
      result.add(getJobDescription(id));
    }
    return result;
  }

  // For debug purpose
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("JobInfo: [");

    for (Map.Entry<Integer, JobInfo> entry : _jobIdToInfo.entrySet()) {
      s.append("{id:" + entry.getKey() + ", value: " + entry.getValue() + "}");
    }

    s.append("]\nStageInfo: [");
    for (Map.Entry<StageAttemptId, StageInfo> entry : _stageIdToInfo.entrySet()) {
      s.append("{id:" + entry.getKey() + ", value: " + entry.getValue() + "}");
    }
    s.append("]");

    return s.toString();
  }

  public static class StageAttemptId {
    public int stageId;
    public int attemptId;

    public StageAttemptId(int stageId, int attemptId) {
      this.stageId = stageId;
      this.attemptId = attemptId;
    }

    @Override
    public int hashCode() {
      return new Integer(stageId).hashCode() * 31 + new Integer(attemptId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof StageAttemptId) {
        StageAttemptId other = (StageAttemptId) obj;
        return stageId == other.stageId && attemptId == other.attemptId;
      }
      return false;
    }

    public String name() {
      return stageId + "." + attemptId;
    }

    public String toString() {
      return "id: " + stageId + " # attemptId: " + attemptId;
    }
  }

  public static class JobInfo {
    public int jobId;
    public String jobGroup;
    public long startTime;
    public long endTime;
    public final List<Integer> stageIds = new ArrayList<Integer>();

    /* Tasks */
    public int numTasks = 0;
    public int numActiveTasks = 0;
    public int numCompletedTasks = 0;
    public int numSkippedTasks = 0;
    public int numFailedTasks = 0;

    /* Stages */
    public int numActiveStages = 0;
    // This needs to be a set instead of a simple count to prevent double-counting of rerun stages:
    public final Set<Integer> completedStageIndices = new HashSet<Integer>();
    public int numSkippedStages = 0;
    public int numFailedStages = 0;

    public void addStageId(int stageId) {
      stageIds.add(stageId);
    }

    public double getFailureRate() {
      return SparkJobProgressData.getFailureRate(numCompletedTasks, numFailedTasks);
    }

    public String toString() {
      return String.format("{jobId:%s, jobGroup:%s, startTime:%s, endTime:%s, numTask:%s, numActiveTasks:%s, "
              + "numCompletedTasks:%s, numSkippedTasks:%s, numFailedTasks:%s, numActiveStages:%s, "
              + "completedStageIndices:%s, stages:%s, numSkippedStages:%s, numFailedStages:%s}", jobId, jobGroup,
          startTime, endTime, numTasks, numActiveTasks, numCompletedTasks, numSkippedTasks, numFailedTasks,
          numActiveStages, getListString(completedStageIndices), getListString(stageIds), numSkippedStages,
          numFailedStages);
    }
  }

  public static class StageInfo {
    public int numActiveTasks;
    public int numCompleteTasks;
    public final Set<Integer> completedIndices = new HashSet<Integer>();
    public Map<Long, TaskData> taskDatas = new HashMap<Long, TaskData>();
    public int numFailedTasks;

    // Total accumulated executor runtime
    public long executorRunTime;
    // Total stage duration
    public long duration;

    // Note, currently calculating I/O speed on stage level does not make sense
    // since we do not have information about specific I/O time.
    public long inputBytes = 0;
    public long outputBytes = 0;
    public long shuffleReadBytes = 0;
    public long shuffleWriteBytes = 0;
    public long memoryBytesSpilled = 0;
    public long diskBytesSpilled = 0;

    public String name;
    public String description;

    public double getFailureRate() {
      return SparkJobProgressData.getFailureRate(numCompleteTasks, numFailedTasks);
    }

    public List<Double> getTaskDataSkews() {
      List<Double> skewnesses = new ArrayList<Double>();
      List<Long> inputBytes = new ArrayList<Long>();
      List<Long> outputBytes = new ArrayList<Long>();
      List<Long> shuffleReadBytes = new ArrayList<Long>();
      List<Long> shuffleWriteBytes = new ArrayList<Long>();

      for (TaskData taskData : taskDatas.values()) {
        if (taskData.taskInfo.status == STATUS.SUCCESS && taskData.taskMetrics != null) {
          inputBytes.add(taskData.taskMetrics.inputMetrics != null ? taskData.taskMetrics.inputMetrics.bytesRead : 0l);
          outputBytes.add(taskData.taskMetrics.outputMetrics != null ? taskData.taskMetrics.outputMetrics.bytesWritten : 0l);
          shuffleReadBytes.add(taskData.taskMetrics.shuffleReadMetrics != null ? taskData.taskMetrics.shuffleReadMetrics.totalBytesRead() : 0l);
          shuffleWriteBytes.add(taskData.taskMetrics.shuffleWriteMetrics != null ? taskData.taskMetrics.shuffleWriteMetrics.shuffleBytesWritten : 0l);
        }
      }

      skewnesses.add(Statistics.computeDataSkew(inputBytes));
      skewnesses.add(Statistics.computeDataSkew(outputBytes));
      skewnesses.add(Statistics.computeDataSkew(shuffleReadBytes));
      skewnesses.add(Statistics.computeDataSkew(shuffleWriteBytes));
      return skewnesses;
    }

    // TODO: accumulables info seem to be unnecessary, might might be useful later on
    // sample code from Spark source: var accumulables = new HashMap[Long, AccumulableInfo]

    @Override
    public String toString() {
      return String.format("{numActiveTasks:%s, numCompleteTasks:%s, completedIndices:%s, numFailedTasks:%s,"
              + " executorRunTime:%s, inputBytes:%s, outputBytes:%s, shuffleReadBytes:%s, shuffleWriteBytes:%s,"
              + " memoryBytesSpilled:%s, diskBytesSpilled:%s, name:%s, description:%s}",
          numActiveTasks, numCompleteTasks, getListString(completedIndices), numFailedTasks, executorRunTime,
          inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes, memoryBytesSpilled, diskBytesSpilled, name,
          description);
    }
  }

  public static class TaskData {
    public TaskInfo taskInfo;
    public TaskMetrics taskMetrics = null;
    public String errorMessage = null;
  }

  public static class TaskInfo {
    public long taskId;
    public int index;
    public int attemptNumber;
    public long launchTime;
    public String executorId;
    public String host;
    public boolean speculative;
    public String taskLocality;
    public long gettingResultTime = 0l;
    public STATUS status = STATUS.UNKNOWN;
    public long finishTime = 0l;

    public TaskInfo(long taskId, int index, int attemptNumber, long launchTime, String executorId,
                    String host, boolean speculative, String taskLocality,
                    long gettingResultTime, long finishTime) {
      this.taskId = taskId;
      this.index = index;
      this.attemptNumber = attemptNumber;
      this.launchTime = launchTime;
      this.executorId = executorId;
      this.host = host;
      this.speculative = speculative;
      this.taskLocality = taskLocality;
      this.gettingResultTime = gettingResultTime;
      this.finishTime = finishTime;
    }

    public TaskInfo(org.apache.spark.scheduler.TaskInfo sparkTaskInfo) {
      this(
              sparkTaskInfo.taskId(),
              sparkTaskInfo.index(),
              sparkTaskInfo.attemptNumber(),
              sparkTaskInfo.launchTime(),
              sparkTaskInfo.executorId(),
              sparkTaskInfo.host(),
              sparkTaskInfo.speculative(),
              sparkTaskInfo.taskLocality().toString(),
              sparkTaskInfo.gettingResultTime(),
              sparkTaskInfo.finishTime()
       );
      if (sparkTaskInfo.successful()) {
        setTaskStatus(STATUS.SUCCESS);
      } else if (sparkTaskInfo.failed()) {
        setTaskStatus(STATUS.FAILED);
      } else if (sparkTaskInfo.running()) {
        setTaskStatus(STATUS.RUNNING);
      } else {
        setTaskStatus(STATUS.UNKNOWN);
      }
    }

    public void setTaskStatus(STATUS status) {
      this.status = status;
    }
  }

  public static class TaskMetrics {
    public String hostname;
    public long executorDeserializeTime;
    public long executorRunTime;
    public long resultSize;
    public long jvmGCTime;
    public long resultSerialization;
    public long memoryBytesSpilled;
    public long diskBytesSpilled;
    public InputMetrics inputMetrics = null;
    public OutputMetrics outputMetrics = null;
    public ShuffleReadMetrics shuffleReadMetrics = null;
    public ShuffleWriteMetrics shuffleWriteMetrics = null;

    public TaskMetrics(String hostname, long executorDeserializeTime, long executorRuntime,
                       long resultSize, long jvmGCTime, long resultSerialization,
                       long memoryBytesSpilled, long diskBytesSpilled) {
      this.hostname = hostname;
      this.executorDeserializeTime = executorDeserializeTime;
      this.executorRunTime = executorRuntime;
      this.resultSize = resultSize;
      this.jvmGCTime = jvmGCTime;
      this.resultSerialization = resultSerialization;
      this.memoryBytesSpilled = memoryBytesSpilled;
      this.diskBytesSpilled = diskBytesSpilled;
    }

    public TaskMetrics(org.apache.spark.executor.TaskMetrics taskMetrics) {
      this(
              taskMetrics.hostname(),
              taskMetrics.executorDeserializeTime(),
              taskMetrics.executorRunTime(),
              taskMetrics.resultSize(),
              taskMetrics.jvmGCTime(),
              taskMetrics.resultSerializationTime(),
              taskMetrics.memoryBytesSpilled(),
              taskMetrics.diskBytesSpilled()
      );

      if (taskMetrics.inputMetrics().nonEmpty()) {
        this.inputMetrics = new InputMetrics(taskMetrics.inputMetrics().get());
      }

      if (taskMetrics.outputMetrics().nonEmpty()) {
        this.outputMetrics = new OutputMetrics(taskMetrics.outputMetrics().get());
      }

      if (taskMetrics.shuffleReadMetrics().nonEmpty()) {
        this.shuffleReadMetrics = new ShuffleReadMetrics(taskMetrics.shuffleReadMetrics().get());
      }

      if (taskMetrics.shuffleWriteMetrics().nonEmpty()) {
        this.shuffleWriteMetrics = new ShuffleWriteMetrics(taskMetrics.shuffleWriteMetrics().get());
      }
    }
  }

  public static class InputMetrics {
    public String readMethod = "";
    public long bytesRead;
    public long recordsRead;

    public InputMetrics(String readMethod, long bytesRead, long recordsRead) {
      this.readMethod = readMethod;
      this.bytesRead = bytesRead;
      this.recordsRead = recordsRead;
    }

    public InputMetrics(org.apache.spark.executor.InputMetrics inputMetrics) {
      this(
              inputMetrics.readMethod().toString(),
              inputMetrics.bytesRead(),
              inputMetrics.recordsRead()
      );
    }
  }

  public static class OutputMetrics {
    public String writeMethod = "";
    public long bytesWritten;
    public long recordsWritten;

    public OutputMetrics(String writeMethod, long bytesWritten, long recordsWritten) {
      this.writeMethod = writeMethod;
      this.bytesWritten = bytesWritten;
      this.recordsWritten = recordsWritten;
    }

    public OutputMetrics(org.apache.spark.executor.OutputMetrics outputMetrics) {
      this(
              outputMetrics.writeMethod().toString(),
              outputMetrics.bytesWritten(),
              outputMetrics.recordsWritten()
      );
    }
  }

  public static class ShuffleReadMetrics {
    public int remoteBlocksFetched;
    public int localBlocksFetched;
    public long fetchWaitTime;
    public long remoteBytesRead;
    public long localBytesRead;
    public long totalRecordsRead;

    public ShuffleReadMetrics(int remoteBlocksFetched, int localBlocksFetched, long fetchWaitTime,
                              long remoteBytesRead, long localBytesRead, long totalRecordsRead) {
      this.remoteBlocksFetched = remoteBlocksFetched;
      this.localBlocksFetched = localBlocksFetched;
      this.fetchWaitTime = fetchWaitTime;
      this.remoteBytesRead = remoteBytesRead;
      this.localBytesRead = localBytesRead;
      this.totalRecordsRead = totalRecordsRead;
    }

    public ShuffleReadMetrics(org.apache.spark.executor.ShuffleReadMetrics shuffleReadMetrics) {
      this(
              shuffleReadMetrics.remoteBlocksFetched(),
              shuffleReadMetrics.localBlocksFetched(),
              shuffleReadMetrics.fetchWaitTime(),
              shuffleReadMetrics.remoteBytesRead(),
              shuffleReadMetrics.localBytesRead(),
              shuffleReadMetrics.totalBytesRead()
      );
    }

    public Long totalBytesRead() {
      return remoteBytesRead + localBytesRead;
    }
  }

  public static class ShuffleWriteMetrics {
    public long shuffleBytesWritten;
    public long shuffleWriteTime;
    public long shuffleRecordsWritten;

    public ShuffleWriteMetrics(long shuffleBytesWritten, long shuffleWriteTime, long shuffleRecordsWritten) {
      this.shuffleBytesWritten = shuffleBytesWritten;
      this.shuffleWriteTime = shuffleWriteTime;
      this.shuffleRecordsWritten = shuffleRecordsWritten;
    }

    public ShuffleWriteMetrics(org.apache.spark.executor.ShuffleWriteMetrics shuffleWriteMetrics) {
      this(
              shuffleWriteMetrics.shuffleBytesWritten(),
              shuffleWriteMetrics.shuffleWriteTime(),
              shuffleWriteMetrics.shuffleRecordsWritten()
      );
    }
  }

  public enum STATUS {
    SUCCESS, FAILED, RUNNING, UNKNOWN
  }

  private static String getListString(Collection collection) {
    return "[" + StringUtils.join(collection, ",") + "]";
  }
  private static String getCollectionString(Collection collection, String separator) {
    return StringUtils.join(collection, separator);
  }
}
