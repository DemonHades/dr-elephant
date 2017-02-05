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

package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.spark.data.SparkApplicationData;
import com.linkedin.drelephant.spark.data.SparkEnvironmentData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import com.linkedin.drelephant.util.Utils;
import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * This heuristic rule check some of the most commonly set spark properties and make sure the user is following
 * a best convention of them.
 */
public class BestPropertiesConventionHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(BestPropertiesConventionHeuristic.class);

  public static final String SPARK_APP_OWNER = "spark.job.owner";
  public static final String SPARK_JOB_TYPE = "spark.job.type";
  public static final String SPARK_MASTER = "spark.master";
  public static final String SPARK_SERIALIZER = "spark.serializer";
  public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
  public static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
  public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
  public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";
  public static final String SPARK_DRIVER_EXTRAJAVAOPTIONS = "spark.driver.extraJavaOptions";
  public static final String SPARK_EXECUTOR_EXTRAJAVAOPTIONS = "spark.executor.extraJavaOptions";
  public static final String SPARK_YARN_AM_EXTRAJAVAOPTIONS = "spark.yarn.am.extraJavaOptions";
  public static final String SPARK_YARN_DRIVER_MEMORYOVERHEAD = "spark.yarn.driver.memoryOverhead";
  public static final String SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = "spark.yarn.executor.memoryOverhead";
  public static final String SPARK_DYNAMICALLOCATION_ENABLED = "spark.dynamicAllocation.enabled";
  public static final String SPARK_DYNAMICALLOCATION_MAXEXECUTORS = "spark.dynamicAllocation.maxExecutors";
  public static final String SPARK_YARN_QUEUE = "spark.yarn.queue";
  public static final String SPARK_DRIVER_HOST = "spark.driver.host";
  public static final String SPARK_YARN_APP_DRIVER_CONTAINER_LOG_DIR = "spark.yarn.app.container.log.dir";
  public static final String NOT_PRESENT = "Not presented";


  private static final Map<String, String> SPARK_PROPERTY = new HashMap<String, String>() {
    {
      put(SPARK_APP_OWNER, "");
      put(SPARK_JOB_TYPE, "");
      put(SPARK_MASTER, "yarn-cluster");
      put(SPARK_SERIALIZER, "JavaSerializer");
      put(SPARK_DRIVER_MEMORY, "1g");
      put(SPARK_EXECUTOR_MEMORY, "1g");
      put(SPARK_EXECUTOR_CORES, "1");
      put(SPARK_SHUFFLE_MANAGER, "SORT");
      put(SPARK_EXECUTOR_INSTANCES, "2");
      put(SPARK_DRIVER_EXTRAJAVAOPTIONS, "");
      put(SPARK_EXECUTOR_EXTRAJAVAOPTIONS, "");
      put(SPARK_YARN_AM_EXTRAJAVAOPTIONS, "");
      put(SPARK_YARN_DRIVER_MEMORYOVERHEAD, String.format("10%% * %s", SPARK_DRIVER_MEMORY));
      put(SPARK_YARN_EXECUTOR_MEMORYOVERHEAD, String.format("10%% * %s", SPARK_EXECUTOR_MEMORY));
      put(SPARK_DYNAMICALLOCATION_ENABLED, "false");
      put(SPARK_DYNAMICALLOCATION_MAXEXECUTORS, NOT_PRESENT);
      put(SPARK_DRIVER_HOST, "");
    }
  };

  // Severity parameters.
  private static final String NUM_CORE_SEVERITY = "num_core_severity";
  private static final String DRIVER_MEM_SEVERITY = "driver_memory_severity_in_gb";

  // Default value of parameters
//  private double[] numCoreLimit= {2d};                   // Spark Executor Cores
//  private double[] driverMemLimits = {4d, 4d, 8d, 8d};   // Spark Driver Memory
  private double[] numCoreLimit= {4d, 4d, 6d, 6d};                   // Spark Executor Cores
  private double[] driverMemLimits = {4d, 8d, 12d, 12d};   // Spark Driver Memory

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confNumCoreLimit = Utils.getParam(paramMap.get(NUM_CORE_SEVERITY), numCoreLimit.length);
    if (confNumCoreLimit != null) {
      numCoreLimit = confNumCoreLimit;
    }
    logger.info(heuristicName + " will use " + NUM_CORE_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(numCoreLimit));

    double[] confDriverMemLimits = Utils.getParam(paramMap.get(DRIVER_MEM_SEVERITY), driverMemLimits.length);
    if (confDriverMemLimits != null) {
      driverMemLimits = confDriverMemLimits;
    }
    logger.info(heuristicName + " will use " + DRIVER_MEM_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(driverMemLimits));
    for (int i = 0; i < driverMemLimits.length; i++) {
      driverMemLimits[i] = (double) MemoryFormatUtils.stringToBytes(Double.toString(driverMemLimits[i]) + "G");
    }
  }

  public BestPropertiesConventionHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkEnvironmentData env = data.getEnvironmentData();
    Map<String, String> sparkProperty = new HashMap<String, String>();
    for (String key : SPARK_PROPERTY.keySet()) {
      sparkProperty.put(key, env.getSparkProperty(key, SPARK_PROPERTY.get(key)));
    }
    // show queue info and driver container log url
    String[] containerLogDir = env.getSparkProperty(SPARK_YARN_APP_DRIVER_CONTAINER_LOG_DIR, "").split("/");
    sparkProperty.put(SPARK_YARN_APP_DRIVER_CONTAINER_LOG_DIR, containerLogDir[containerLogDir.length - 1]);
    sparkProperty.put(SPARK_YARN_QUEUE, env.getSparkProperty(SPARK_YARN_QUEUE, ""));

    int coreNum = sparkProperty.get(SPARK_EXECUTOR_CORES) == null ?
            1 : Integer.parseInt(sparkProperty.get(SPARK_EXECUTOR_CORES));

    Severity kryoSeverity =
        binarySeverity("org.apache.spark.serializer.KryoSerializer",
                sparkProperty.get(SPARK_SERIALIZER), true, Severity.MODERATE);
    Severity driverMemSeverity =
            getDriverMemorySeverity(MemoryFormatUtils.stringToBytes(sparkProperty.get(SPARK_DRIVER_MEMORY)));
    Severity sortSeverity = binarySeverity("sort", sparkProperty.get(SPARK_SHUFFLE_MANAGER), true, Severity.MODERATE);
    Severity executorCoreSeverity = getCoreNumSeverity(coreNum);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), Severity.max(kryoSeverity, driverMemSeverity, sortSeverity,
        executorCoreSeverity), 0);

    for (Map.Entry<String, String> entry : sparkProperty.entrySet()) {
      String detail = entry.getValue();
      if (entry.getKey().endsWith("extraJavaOptions"))
        detail = StringUtils.join(entry.getValue().split("\\s+"), "\n");
      result.addResultDetail(entry.getKey(), propertyToString(detail));
    }

    return result;
  }

  private Severity getCoreNumSeverity(int cores) {
    return Severity.getSeverityAscending(
            cores, numCoreLimit[0], numCoreLimit[1], numCoreLimit[2], numCoreLimit[3]);
  }

  private Severity getDriverMemorySeverity(long mem) {
    return Severity.getSeverityAscending(
        mem, driverMemLimits[0], driverMemLimits[1], driverMemLimits[2], driverMemLimits[3]);
  }

  private static Severity binarySeverity(String expectedValue, String actualValue, boolean ignoreNull,
      Severity severity) {
    if (actualValue == null) {
      if (ignoreNull) {
        return Severity.NONE;
      } else {
        return severity;
      }
    }

    if (actualValue.equals(expectedValue)) {
      return Severity.NONE;
    } else {
      return severity;
    }
  }

  private static String propertyToString(String val) {
    return val == null ? "Not presented. Using default" : val;
  }
}
