/*
 * Copyright 2025 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.tasks;

import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.dto.StepTagPermit;
import com.netflix.maestro.engine.dto.StepUuidSeq;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.properties.TagPermitTaskProperties;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.runtime.FlowTask;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.models.MessageDto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro Tag permit task is an internal step to scan and acquire tag permits for user steps. This
 * is an internal task and should not be used externally by users.
 */
@Slf4j
public final class MaestroTagPermitTask implements FlowTask {
  /** Status code for step acquired tag permits. */
  public static final int ACQUIRED_STATUS_CODE = 7;

  /** Default action code. */
  public static final int DEFAULT_ACTION_CODE = 0;

  /** Action code for deleting tag permit. */
  public static final int TAG_PERMIT_DELETE_CODE = 1;

  /** Action code for deleting step tag permit record. */
  public static final int STEP_TAG_PERMIT_DELETE_CODE = 2;

  /** Action code for tag permit limit change. */
  public static final int TAG_PERMIT_CHANGE_CODE = 3;

  /** Action code for new step tag permit acquiring. */
  public static final int STEP_TAG_PERMIT_CHANGE_CODE = 4;

  private final MaestroTagPermitDao tagPermitDao;
  private final TagPermitTaskProperties properties;
  private final MaestroQueueSystem queueSystem;
  private final MaestroMetrics metrics;

  private static final class TagPermitTaskState {
    // key is tag, val is maxAllowed permits
    private final Map<String, Integer> tagPermits = new HashMap<>();
    // for tag permit not saved in DB and come from the tag directly.
    private final Map<String, Integer> tmpTagPermits = new HashMap<>();

    // key is tag, val is set of step uuids using this tag
    private final Map<String, Set<UUID>> tagUsage = new HashMap<>();
    // key is step uuid, val is set of tags used by this step
    private final Map<UUID, Set<String>> stepHoldingTags = new HashMap<>();

    // key is seq_num, value is step tag permit
    private final SortedMap<Long, StepTagPermit> queuedSteps = new TreeMap<>();

    private long lastCleanupTs = 0;
    private long maxSeqNum = 0;

    private void addTagPermits(List<TagPermit> tps) {
      tps.forEach(tp -> tagPermits.put(tp.getTag(), tp.getMaxAllowed()));
    }

    private void removeTagPermit(String tag) {
      tagPermits.remove(tag);
    }

    private void removeStep(StepUuidSeq stepUuidSeq) {
      var tags = stepHoldingTags.remove(stepUuidSeq.uuid());
      if (tags != null) {
        for (String tag : tags) {
          tagUsage.get(tag).remove(stepUuidSeq.uuid());
          if (tagUsage.get(tag).isEmpty()) {
            tagUsage.remove(tag);
            tmpTagPermits.remove(tag);
          }
        }
      }
      queuedSteps.remove(stepUuidSeq.seqNum());
    }

    private void addStepTagPermit(StepTagPermit stp) {
      if (stp.status() == ACQUIRED_STATUS_CODE) { // acquired
        stepHoldingTags.put(stp.uuid(), new HashSet<>(List.of(stp.tags())));
        for (int i = 0; i < stp.tags().length; ++i) {
          String tag = stp.tags()[i];
          tagUsage.computeIfAbsent(tag, t -> new HashSet<>()).add(stp.uuid());

          Integer limit = stp.limits() == null ? null : stp.limits()[i];
          if (limit != null) {
            tmpTagPermits.put(tag, limit);
          }
        }
      } else { // queued
        queuedSteps.put(stp.seqNum(), stp);
      }
      if (stp.seqNum() > maxSeqNum) {
        maxSeqNum = stp.seqNum();
      }
    }

    private boolean reachedLimit(String tag, Integer limit) {
      Integer maxAllowed = tagPermits.get(tag);
      if (maxAllowed == null) {
        maxAllowed = tmpTagPermits.getOrDefault(tag, limit);
      }
      if (maxAllowed == null) {
        return false; // no limit
      }
      int used = tagUsage.getOrDefault(tag, Set.of()).size();
      return used >= maxAllowed;
    }

    private void assignTagPermits(
        MaestroTagPermitDao tagPermitDao, MaestroQueueSystem queueSystem, MaestroMetrics metrics) {
      List<Long> toDelete = new ArrayList<>();
      for (Map.Entry<Long, StepTagPermit> entry : queuedSteps.entrySet()) {
        StepTagPermit stp = entry.getValue();
        if (stepHoldingTags.containsKey(stp.uuid())) {
          toDelete.add(entry.getKey());
          continue;
        }
        boolean canAcquire = true;
        for (int i = 0; i < stp.tags().length; ++i) {
          String tag = stp.tags()[i];
          Integer limit = stp.limits()[i];
          if (reachedLimit(tag, limit)) {
            canAcquire = false;
            break;
          }
        }
        if (canAcquire) {
          boolean acquired = tagPermitDao.markStepTagPermitAcquired(stp.uuid());
          if (!acquired) {
            continue;
          }
          addStepTagPermit(
              new StepTagPermit(
                  stp.uuid(), stp.seqNum(), ACQUIRED_STATUS_CODE, stp.tags(), null, null));
          toDelete.add(entry.getKey());

          var msg = // wake up the step
              MessageDto.createMessageForWakeUp(
                  stp.event().getInfo(),
                  stp.event().getMessage(),
                  stp.event().getAuthor().getName(),
                  DEFAULT_ACTION_CODE);
          queueSystem.notify(msg);
          metrics.counter(MetricConstants.TAG_PERMIT_ACQUIRED_METRIC, getClass());
        }
      }
      toDelete.forEach(queuedSteps::remove);
    }
  }

  /** Constructor. */
  public MaestroTagPermitTask(
      MaestroTagPermitDao tagPermitDao,
      TagPermitTaskProperties properties,
      MaestroQueueSystem queueSystem,
      MaestroMetrics metrics) {
    this.tagPermitDao = tagPermitDao;
    this.properties = properties;
    this.queueSystem = queueSystem;
    this.metrics = metrics;
  }

  /** start will load all tag permit info from database. */
  @Override
  public void start(Flow flow, Task task) {
    long start = System.nanoTime();
    TagPermitTaskState state = new TagPermitTaskState();

    // load all synced tag permit and step tag permit info from database to memory
    loadTagPermitsToState(state);
    loadStepTagPermitsToState(state);

    long duration = System.nanoTime() - start;
    LOG.info("took [{}] ns to load tag permits and step tag permits", duration);
    metrics.timer(MetricConstants.TAG_PERMIT_START_DURATION_METRIC, duration, getClass());

    task.getOutputData().put(Constants.STEP_RUNTIME_SUMMARY_FIELD, state);
  }

  private void loadTagPermitsToState(TagPermitTaskState state) {
    int cnt = properties.getBatchSize();
    String cursor = "";
    while (cnt == properties.getBatchSize()) {
      List<TagPermit> res = tagPermitDao.getSyncedTagPermits(cursor, properties.getBatchSize());
      if (!res.isEmpty()) {
        state.addTagPermits(res);
        cursor = res.getLast().getTag();
      }
      cnt = res.size();
    }
  }

  private void loadStepTagPermitsToState(TagPermitTaskState state) {
    int loaded = properties.getBatchSize();
    UUID cursor = null;
    while (loaded == properties.getBatchSize()) {
      List<StepTagPermit> res = tagPermitDao.loadStepTagPermits(cursor, properties.getBatchSize());
      if (!res.isEmpty()) {
        res.forEach(state::addStepTagPermit);
        cursor = res.getLast().uuid();
      }
      loaded = res.size();
    }
  }

  /**
   * Execute will load unknown records, also, reload tag permit limit if longer than 30sec, etc.
   *
   * <p>Maestro tag permit task leverages the task code to indicate the action:
   * <li>- default action code, e.g. normal scan: code = 0
   * <li>- tag permit delete notification: code = 1
   * <li>- step tag permit release notification: code = 2
   * <li>- tag permit change notification: code = 3
   * <li>- new acquiring step tag permit request: code = 4
   */
  @Override
  public boolean execute(Flow flow, Task task) {
    int code = task.getCode();
    TagPermitTaskState state =
        (TagPermitTaskState) task.getOutputData().get(Constants.STEP_RUNTIME_SUMMARY_FIELD);
    cleanUpIfNeeded(state, code);

    if (code == DEFAULT_ACTION_CODE || code == TAG_PERMIT_CHANGE_CODE) {
      markAndLoadTagPermitsToState(state);
    }
    if (code == DEFAULT_ACTION_CODE || code == STEP_TAG_PERMIT_CHANGE_CODE) {
      markAndLoadStepTagPermitsToState(state);
    }

    state.assignTagPermits(tagPermitDao, queueSystem, metrics);

    task.setStartDelayInMillis(properties.getScanInterval());
    metrics.counter(MetricConstants.TAG_PERMIT_EXECUTION_METRIC, getClass());
    return false;
  }

  private void cleanUpIfNeeded(TagPermitTaskState state, int code) {
    boolean needCleanup =
        System.currentTimeMillis() - state.lastCleanupTs >= properties.getCleanUpInterval();
    if (code == TAG_PERMIT_DELETE_CODE || needCleanup) {
      int cnt = properties.getBatchSize();
      while (cnt == properties.getBatchSize()) {
        List<String> tags = tagPermitDao.removeTagPermits(properties.getBatchSize());
        tags.forEach(state::removeTagPermit);
        cnt = tags.size();
        LOG.info("Deleted [{}] tag permits", cnt);
      }
    }
    if (code == STEP_TAG_PERMIT_DELETE_CODE || needCleanup) {
      int cnt = properties.getBatchSize();
      while (cnt == properties.getBatchSize()) {
        var res = tagPermitDao.removeReleasedStepTagPermits(properties.getBatchSize());
        res.forEach(state::removeStep);
        cnt = res.size();
        LOG.info("Deleted [{}] step tag permit status records", cnt);
      }
    }
    if (needCleanup) {
      state.lastCleanupTs = System.currentTimeMillis();
    }
  }

  private void markAndLoadTagPermitsToState(TagPermitTaskState state) {
    int cnt = properties.getBatchSize();
    while (cnt == properties.getBatchSize()) {
      var res = tagPermitDao.markAndLoadTagPermits(properties.getBatchSize());
      state.addTagPermits(res);
      cnt = res.size();
      LOG.info("Marked and loaded [{}] tag permits synced", cnt);
    }
  }

  private void markAndLoadStepTagPermitsToState(TagPermitTaskState state) {
    int cnt = properties.getBatchSize();
    while (cnt == properties.getBatchSize()) {
      var res = tagPermitDao.markAndLoadStepTagPermits(state.maxSeqNum, properties.getBatchSize());
      res.forEach(state::addStepTagPermit);
      cnt = res.size();
      LOG.info("Marked and loaded [{}] step tag permits synced", cnt);
    }
  }
}
