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

package com.linkedin.drelephant;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.*;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import com.linkedin.drelephant.util.XMHandler;
import controllers.MetricsController;
import models.AppResult;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.InetAddress;


/**
 * The class that runs the Dr. Elephant daemon
 */
public class ElephantRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);

  private static final long FETCH_INTERVAL = 60 * 1000;     // Interval between fetches
  private static final long RETRY_INTERVAL = 60 * 1000;     // Interval between retries
  private static final int EXECUTOR_NUM = 5;                // The number of executor threads to analyse the jobs

  private static final String FETCH_INTERVAL_KEY = "drelephant.analysis.fetch.interval";
  private static final String RETRY_INTERVAL_KEY = "drelephant.analysis.retry.interval";
  private static final String EXECUTOR_NUM_KEY = "drelephant.analysis.thread.count";

  private AtomicBoolean _running = new AtomicBoolean(true);
  private long lastRun;
  private long _fetchInterval;
  private long _retryInterval;
  private int _executorNum;
  private HadoopSecurity _hadoopSecurity;
  private ThreadPoolExecutor _threadPoolExecutor;
  private AnalyticJobGenerator _analyticJobGenerator;

  // add "HOPE~" by biyan
  private static final String HOPE_LAUNCH_PORT = "drelephant.hope.launch.port";
  private static final String HOPE_NOTIFY_ENABLE = "drelephant.analysis.hope.notify.enable";
  private static final String HOPE_NOTIFY_ADMIN = "drelephant.analysis.hope.notify.admin";
  private static final String HOPE_NOTIFY_COMMON_USER = "drelephant.analysis.hope.notify.common";
  private static final String HOPE_NOTIFY_JOBTYPE = "drelephant.analysis.hope.notify.jobtype";

  private String _hopeLaunchPort;
  private boolean _hopeNotifyEnable;
  private String _hopeNotifyAdmin;
  private String _hopeNotifyJobType;
  private String _hopeNotifyCommonUser;
  private String _hopeNotifyMsgFormat = "嗨, %s: 作业（%s）执行完成，可点击查看[作业性能评估报告|%s]，并基于此进一步优化作业。" +
          "\n对性能报告如有任何疑问、建议或意见，请联系%s.";

  private void loadGeneralConfiguration() {
    Configuration configuration = ElephantContext.instance().getGeneralConf();

    _executorNum = Utils.getNonNegativeInt(configuration, EXECUTOR_NUM_KEY, EXECUTOR_NUM);
    _fetchInterval = Utils.getNonNegativeLong(configuration, FETCH_INTERVAL_KEY, FETCH_INTERVAL);
    _retryInterval = Utils.getNonNegativeLong(configuration, RETRY_INTERVAL_KEY, RETRY_INTERVAL);

    _hopeLaunchPort = configuration.get(HOPE_LAUNCH_PORT, "8080");
    _hopeNotifyEnable = configuration.getBoolean(HOPE_NOTIFY_ENABLE, false);
    _hopeNotifyAdmin = configuration.get(HOPE_NOTIFY_ADMIN, "");
    _hopeNotifyJobType = configuration.get(HOPE_NOTIFY_JOBTYPE, "");
    _hopeNotifyCommonUser = configuration.get(HOPE_NOTIFY_COMMON_USER, "");
  }

  private void loadAnalyticJobGenerator() {
    if (HadoopSystemContext.isHadoop2Env()) {
      _analyticJobGenerator = new AnalyticJobGeneratorHadoop2();
    } else {
      throw new RuntimeException("Unsupported Hadoop major version detected. It is not 2.x.");
    }

    try {
      _analyticJobGenerator.configure(ElephantContext.instance().getGeneralConf());
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    logger.info("Dr.elephant has started");
    try {
      _hadoopSecurity = new HadoopSecurity();
      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          HDFSContext.load();
          loadGeneralConfiguration();
          loadAnalyticJobGenerator();
          ElephantContext.init();

          // Initialize the metrics registries.
          MetricsController.init();

          logger.info("executor num is " + _executorNum);
          if (_executorNum < 1) {
            throw new RuntimeException("Must have at least 1 worker thread.");
          }
          ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("dr-el-executor-thread-%d").build();
          _threadPoolExecutor = new ThreadPoolExecutor(_executorNum, _executorNum, 0L, TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<Runnable>(), factory);

          while (_running.get() && !Thread.currentThread().isInterrupted()) {
            _analyticJobGenerator.updateResourceManagerAddresses();
            lastRun = System.currentTimeMillis();

            logger.info("Fetching analytic job list...");

            try {
              _hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            List<AnalyticJob> todos;
            try {
              todos = _analyticJobGenerator.fetchAnalyticJobs();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            for (AnalyticJob analyticJob : todos) {
              _threadPoolExecutor.submit(new ExecutorJob(analyticJob));
            }

            int queueSize = _threadPoolExecutor.getQueue().size();
            MetricsController.setQueueSize(queueSize);
            logger.info("Job queue size is " + queueSize);

            //Wait for a while before next fetch
            waitInterval(_fetchInterval);
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (Exception e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));
    }
  }

  private class ExecutorJob implements Runnable {

    private AnalyticJob _analyticJob;

    ExecutorJob(AnalyticJob analyticJob) {
      _analyticJob = analyticJob;
    }

    @Override
    public void run() {
      try {
        String analysisName = String.format("%s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId());
        long analysisStartTimeMillis = System.currentTimeMillis();
        logger.info(String.format("Analyzing %s", analysisName));
        AppResult result = _analyticJob.getAnalysis();
        result.save();

        if (_hopeNotifyEnable) {
          notifyOwner(_analyticJob);
        }
        long processingTime = System.currentTimeMillis() - analysisStartTimeMillis;
        logger.info(String.format("Analysis of %s took %sms", analysisName, processingTime));
        MetricsController.setJobProcessingTime(processingTime);
        MetricsController.markProcessedJobs();

      } catch (InterruptedException e) {
        logger.info("Thread interrupted");
        logger.info(e.getMessage());
        logger.info(ExceptionUtils.getStackTrace(e));

        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error(e.getMessage());
        logger.error(ExceptionUtils.getStackTrace(e));

        if (_analyticJob != null && _analyticJob.retry()) {
          logger.error("Add analytic job id [" + _analyticJob.getAppId() + "] into the retry list.");
          _analyticJobGenerator.addIntoRetries(_analyticJob);
        } else {
          if (_analyticJob != null) {
            MetricsController.markSkippedJob();
            logger.error("Drop the analytic job. Reason: reached the max retries for application id = ["
                    + _analyticJob.getAppId() + "].");
          }
        }
      }
    }
  }

  private void notifyOwner(AnalyticJob analyticJob) {
    logger.warn("Meituan Job AppID: " + analyticJob.getAppId()
            + "\tJob Name: " + analyticJob.getName()
            + "\tJob Owner: " + analyticJob.getOwner()
            + "\tJob Source: " + analyticJob.getSource());
    String receiver = analyticJob.getOwner();
    if (analyticJob.getSource().equals(_hopeNotifyJobType)
            && !receiver.equals("")
            && !receiver.equals(_hopeNotifyCommonUser)) {
      String[] receivers = StringUtils.split(receiver, ",");
      String msg = String.format(_hopeNotifyMsgFormat, receiver, analyticJob.getName(),
              getSearchURL(analyticJob.getAppId()), _hopeNotifyAdmin);
      XMHandler.sendMessage(msg, receivers);
      if (!_hopeNotifyAdmin.isEmpty())
        XMHandler.sendMessage("少爷：请确认！\n" + msg, _hopeNotifyAdmin);
    }
  }

  private String getSearchURL(String appId) {
    try {
      InetAddress netAddress = InetAddress.getLocalHost();
      String searchUrl = "http://" + netAddress.getHostAddress() + ":" + _hopeLaunchPort + "/search?id=" + appId;
      return searchUrl;
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return "http://10.16.32.101:8080/search?id=" + appId;
  }

  private void waitInterval(long interval) {
    // Wait for long enough
    long nextRun = lastRun + interval;
    long waitTime = nextRun - System.currentTimeMillis();

    if (waitTime <= 0) {
      return;
    }

    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void kill() {
    _running.set(false);
    if (_threadPoolExecutor != null) {
      _threadPoolExecutor.shutdownNow();
    }
  }
}
