/*
 * Copyright 2024 Netflix, Inc.
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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.utils.Checks;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;

/** Foreach step overview to track the progress in an optimized way. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "checkpoint",
      "stats",
      "running_stats",
      "rollup",
      "running_rollup",
      "details",
      "restart_info"
    },
    alphabetic = true)
@Data
public class ForeachStepOverview {
  // checkpoint of the current first non-terminated iteration of the base run (no restarting)
  private long checkpoint;
  private EnumMap<WorkflowInstance.Status, Long> stats;
  private EnumMap<WorkflowInstance.Status, Long> runningStats;

  private WorkflowRollupOverview rollup;
  private WorkflowRollupOverview runningRollup;

  private ForeachDetails details;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private Set<Long> restartInfo; // keep the restarted iteration ids

  private void addTerminalOne(WorkflowInstance.Status status, WorkflowRollupOverview overview) {
    if (stats == null) {
      stats = new EnumMap<>(WorkflowInstance.Status.class);
    }
    stats.put(status, stats.getOrDefault(status, 0L) + 1);

    if (this.rollup == null) {
      this.rollup = new WorkflowRollupOverview();
    }
    this.rollup.aggregate(overview);
  }

  private void addRunningOne(WorkflowInstance.Status status, WorkflowRollupOverview overview) {
    if (runningStats == null) {
      runningStats = new EnumMap<>(WorkflowInstance.Status.class);
    }
    runningStats.put(status, runningStats.getOrDefault(status, 0L) + 1);

    if (runningRollup == null) {
      runningRollup = new WorkflowRollupOverview();
    }
    runningRollup.aggregate(overview);
  }

  /** Add given iteration info including its rollup overview to the foreach step overview. */
  @JsonIgnore
  public void addOne(
      long iterationId, WorkflowInstance.Status status, WorkflowRollupOverview overview) {
    if (status.isTerminal()) {
      addTerminalOne(status, overview);
    } else {
      addRunningOne(status, overview);
    }
    if (details == null) {
      details = new ForeachDetails(new EnumMap<>(WorkflowInstance.Status.class));
    }
    details.add(iterationId, status);
  }

  /** refresh details of foreach step. */
  @JsonIgnore
  public void refreshDetail() {
    if (details != null) {
      details.refresh();
    }
  }

  /**
   * Get merged rollup info from complete iterations and running iterations aggregated from run1 to
   * current run.
   */
  @JsonIgnore
  public WorkflowRollupOverview getOverallRollup() {
    WorkflowRollupOverview overall = new WorkflowRollupOverview();
    if (rollup != null) {
      overall.aggregate(rollup);
    }
    if (runningRollup != null) {
      overall.aggregate(runningRollup);
    }
    return overall;
  }

  /**
   * Get iteration count from the running stats map.
   *
   * @param strict if true, count the total number of instances after the smallest running
   *     iterations, otherwise, only count the non-terminal instances. strict_ordering needs to set
   *     strict to be true.
   * @return the iteration count.
   */
  @JsonIgnore
  public long getRunningStatsCount(boolean strict) {
    if (runningStats == null) {
      return 0L;
    }
    if (strict) {
      Set<Map.Entry<WorkflowInstance.Status, List<Long>>> detailSet =
          details.flatten(e -> true).entrySet();
      long maxIterationId =
          detailSet.stream().flatMap(e -> e.getValue().stream()).reduce(0L, Long::max);
      return maxIterationId
          - detailSet.stream()
              .filter(e -> !e.getKey().isTerminal())
              .flatMap(e -> e.getValue().stream())
              .reduce(maxIterationId + 1, Long::min)
          + 1;
    } else {
      return runningStats.values().stream().reduce(0L, Long::sum);
    }
  }

  /** Check if there is any iteration in a given status. */
  @JsonIgnore
  public boolean statusExistInIterations(WorkflowInstance.Status status) {
    boolean flag = false;
    if (stats != null && stats.containsKey(status)) {
      flag = stats.get(status) > 0;
    }
    if (flag || runningStats == null || !runningStats.containsKey(status)) {
      return flag;
    }
    return runningStats.get(status) > 0;
  }

  /** Clear the data for running iterations. */
  @SuppressWarnings({"PMD.NullAssignment"})
  @JsonIgnore
  public void resetRunning() {
    this.runningStats = null;
    this.runningRollup = null;
  }

  @JsonIgnore
  public boolean isForeachIterationRestartable(long iterationId) {
    if (restartInfo != null && restartInfo.contains(iterationId)) {
      return false;
    }
    if (details == null) {
      return false;
    }
    return details.isForeachIterationRestartable(iterationId);
  }

  @JsonIgnore
  public void updateForRestart(
      long iterationId,
      WorkflowInstance.Status newStatus,
      WorkflowInstance.Status oldStatus,
      WorkflowRollupOverview oldOverview) {
    Checks.checkTrue(
        isForeachIterationRestartable(iterationId) && stats != null && stats.get(oldStatus) > 0,
        "Invalid: pending action tries to restart a non-restartable iteration: " + iterationId);
    long cnt = stats.get(oldStatus);
    if (cnt > 1) {
      stats.put(oldStatus, stats.get(oldStatus) - 1);
    } else {
      stats.remove(oldStatus);
    }
    addRunningOne(newStatus, null);
    rollup.segregate(oldOverview);
    details.resetIterationDetail(iterationId, newStatus, oldStatus);
    if (restartInfo == null) {
      restartInfo = new HashSet<>();
    }
    restartInfo.add(iterationId);
  }

  @JsonIgnore
  public long getFirstRestartIterationId() {
    if (restartInfo != null && !restartInfo.isEmpty()) {
      return restartInfo.stream().min(Long::compare).get();
    }
    return 0;
  }

  /**
   * This method returns a set of iteration ids, including both iterations (no less than the
   * checkpoint), which are in the terminal state, and the restarted iterations. When we refresh the
   * iteration overview, we don't need to check the status of those iterations in the base run.
   */
  @JsonIgnore
  public Set<Long> getSkippedIterationsWithCheckpoint() {
    Set<Long> skipList = new HashSet<>();
    if (restartInfo != null) {
      skipList.addAll(restartInfo);
    }
    if (details != null) {
      details.flatten(WorkflowInstance.Status::isTerminal).entrySet().stream()
          .flatMap(e -> e.getValue().stream())
          .filter(i -> i >= checkpoint)
          .forEach(skipList::add);
    }
    return skipList;
  }

  /**
   * This method scans the previous foreach step overview and then find out the maximum iteration id
   * launched previously. It also sets its own details by using all the successful iterations from
   * the previous overview minus the specific iteration to reset.
   *
   * @param prev the previous ForeachStepOverview
   * @param toReset the foreach iteration to be reset and will be restarted no matter of its status
   * @return the maximum foreach iteration id
   */
  @JsonIgnore
  public long initiateAndGetByPrevMaxIterationId(ForeachStepOverview prev, long toReset) {
    if (prev == null || prev.details == null) {
      return 0;
    }

    long maxIterationId =
        prev.details.flatten(e -> true).values().stream()
            .flatMap(Collection::stream)
            .reduce(0L, Long::max);

    // We need to keep only succeeded instances as we will restart the remaining instances when
    // restarting all iterations from incomplete. When there is specific iteration to restart
    // we keep all terminal state instances.
    Map<WorkflowInstance.Status, List<Long>> toKeep =
        prev.details.flatten(
            toReset == 0
                ? (e -> e == WorkflowInstance.Status.SUCCEEDED)
                : (WorkflowInstance.Status::isTerminal));

    // checkpoint can be set to the first restartable instance id - 1
    List<Long> instanceIds = toKeep.values().stream().flatMap(Collection::stream).sorted().toList();
    checkpoint = instanceIds.size();
    for (int i = 0; i < instanceIds.size(); ++i) {
      if (instanceIds.get(i) != i + 1) {
        checkpoint = i;
        break;
      }
    }

    toKeep.forEach((status, ids) -> ids.forEach(i -> addOne(i, status, null)));
    refreshDetail();

    return maxIterationId;
  }

  /**
   * Initializes step rollup with "base rollup". "Base rollup" consists of rollup of steps completed
   * as parts of previous runs. It's calculated by taking aggregated values of previous run rollups
   * of the iterations that completed in the previous run and that are going to be restarted as part
   * of the new run - and subtracting it from foreach step rollup of the previous run. This way we
   * get the delta that equals to what we want to preserve from the previous runs.
   *
   * <p>todo logic here can be merged with {@link
   * ForeachStepOverview#initiateAndGetByPrevMaxIterationId} to initialize both details and rollup
   * at the same time. we'd need to rewrite the details aggregation logic to be segregation.
   *
   * @param prevRollup foreach step rollup of the previous run
   * @param aggregatedRollupsPrevRun aggregated values of previous run rollups of the iterations
   *     that completed in the previous run and that are going to be restarted as part of the new
   *     run
   */
  public void initiateStepRollup(
      WorkflowRollupOverview prevRollup, WorkflowRollupOverview aggregatedRollupsPrevRun) {
    if (aggregatedRollupsPrevRun == null || aggregatedRollupsPrevRun.getTotalLeafCount() == 0) {
      setRollup(prevRollup);
      return;
    }
    Checks.checkTrue(
        prevRollup != null,
        "Previous run rollup cannot be null if to-be-restarted rollup is not empty.");

    // should never breach unless there is a bug
    Checks.checkTrue(
        prevRollup.getTotalLeafCount() >= aggregatedRollupsPrevRun.getTotalLeafCount(),
        String.format(
            "Rollup of the previous run cannot have less steps [%s] than rollup of the "
                + "previous steps that are also expected to execute in the new run [%s].",
            prevRollup.getTotalLeafCount(), aggregatedRollupsPrevRun.getTotalLeafCount()));

    WorkflowRollupOverview baseRollup = new WorkflowRollupOverview();
    baseRollup.aggregate(prevRollup);
    baseRollup.segregate(aggregatedRollupsPrevRun);

    if (baseRollup.getTotalLeafCount() == 0) {
      return;
    }

    if (rollup == null) {
      setRollup(baseRollup);
    } else {
      rollup.aggregate(baseRollup);
    }
  }

  /**
   * Get iterations that are going to be run the current run. We do that by getting the difference
   * between initialized details in the current run and prev run. Current run details contains
   * iterations we want to keep from the previous run, e.g. "aggregated base" for details.
   *
   * @param prevStepOverview previous run step overview
   * @return all the instance ids that will be run in the new run that also ran in the prev run
   */
  public Set<Long> getIterationsToRunFromDetails(ForeachStepOverview prevStepOverview) {
    if (prevStepOverview == null || prevStepOverview.details == null) {
      return new HashSet<>();
    }
    Set<Long> currRunInstances =
        details == null
            ? new HashSet<>()
            : details.flatten(e -> true).values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    return prevStepOverview.details.flatten(e -> true).values().stream()
        .flatMap(List::stream)
        .filter(e -> !currRunInstances.contains(e))
        .collect(Collectors.toSet());
  }

  @JsonIgnore
  public Set<Long> getSkippedIterationsInRange(int startInstanceId, Long ancestorMax) {
    if (details == null || ancestorMax == null || ancestorMax.intValue() < startInstanceId) {
      return Collections.emptySet();
    }
    return details
        .flatten(e -> e == WorkflowInstance.Status.SUCCEEDED)
        .getOrDefault(WorkflowInstance.Status.SUCCEEDED, Collections.emptyList())
        .stream()
        .filter(iterId -> iterId >= startInstanceId)
        .collect(Collectors.toSet());
  }
}
