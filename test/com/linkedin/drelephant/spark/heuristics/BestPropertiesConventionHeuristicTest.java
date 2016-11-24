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

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.spark.MockSparkApplicationData;
import com.linkedin.drelephant.spark.data.SparkApplicationData;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.drelephant.spark.heuristics.BestPropertiesConventionHeuristic.*;


/**
 * This class test the BestPropertiesConventionHeuristic
 *
 */
public class BestPropertiesConventionHeuristicTest extends TestCase {
  public void testPropertiesCheck() {
    assertEquals(analyzeJob(getDefaultGoodProperteis()), Severity.NONE);

    assertEquals(Severity.MODERATE, analyzeJob(getPropertiesAndOverideOne(SPARK_DRIVER_MEMORY, "7G")));
    assertEquals(Severity.SEVERE, analyzeJob(getPropertiesAndOverideOne(SPARK_DRIVER_MEMORY, "10G")));
    assertEquals(Severity.CRITICAL, analyzeJob(getPropertiesAndOverideOne(SPARK_DRIVER_MEMORY, "12G")));

    assertEquals(Severity.NONE, analyzeJob(getPropertiesAndOverideOne(SPARK_EXECUTOR_CORES, "1")));
    assertEquals(Severity.NONE, analyzeJob(getPropertiesAndOverideOne(SPARK_EXECUTOR_CORES, "4")));
    assertEquals(Severity.CRITICAL, analyzeJob(getPropertiesAndOverideOne(SPARK_EXECUTOR_CORES, "8")));


    assertEquals(Severity.MODERATE, analyzeJob(getPropertiesAndOverideOne(SPARK_SERIALIZER, "foo")));
    assertEquals(Severity.MODERATE, analyzeJob(getPropertiesAndOverideOne(SPARK_SHUFFLE_MANAGER, "hash")));
  }

  public void testNullSettings() {
    assertEquals(Severity.NONE, analyzeJob(getPropertiesAndRemove(SPARK_SERIALIZER)));
    assertEquals(Severity.NONE, analyzeJob(getPropertiesAndRemove(SPARK_SHUFFLE_MANAGER)));
    assertEquals(Severity.NONE, analyzeJob(getPropertiesAndRemove(SPARK_EXECUTOR_CORES)));
  }

  private static Properties getDefaultGoodProperteis() {
    Properties properties = new Properties();
    properties.put(SPARK_DRIVER_MEMORY, "1G");
    properties.put(SPARK_EXECUTOR_CORES, "1");
    properties.put(SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");
    properties.put(SPARK_SHUFFLE_MANAGER, "sort");

    return properties;
  }

  private static Properties getPropertiesAndOverideOne(String key, String value) {
    Properties properties = getDefaultGoodProperteis();
    properties.put(key, value);
    return properties;
  }

  private static Properties getPropertiesAndRemove(String key) {
    Properties properties = getDefaultGoodProperteis();
    properties.remove(key);
    return properties;
  }

  private Severity analyzeJob(Properties sparkProperties) {
    SparkApplicationData data = new MockSparkApplicationData();
    for (String key : sparkProperties.stringPropertyNames()) {
      data.getEnvironmentData().addSparkProperty(key, sparkProperties.getProperty(key));
    }
    Map<String, String> paramsMap = new HashMap<String, String>();
    return new BestPropertiesConventionHeuristic(new HeuristicConfigurationData("test_heuristic", "test_class",
        "test_view", new ApplicationType("test_apptype"), paramsMap)).apply(data).getSeverity();
  }
}
