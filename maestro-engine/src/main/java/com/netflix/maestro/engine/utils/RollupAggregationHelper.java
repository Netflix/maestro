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
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.artifact.WhileArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 * Helper class for computing aggregated rollup. Currently, rollup will be inaccurate in the case
 * when one or more of the foreach iterations are restarted.
 */
@AllArgsConstructor
public class RollupAggregationHelper {

  private final MaestroStepInstanceDao stepInstanceDao;

  /**
   * Gets foreach and subworkflow step rollups for steps that are not in the new/latest run, but
   * were completed in the previous run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepIdRunIdLoopSubworkflowPrevious stepId to runId map for subworkflow and foreach steps
   *     from the previous runs
   * @return aggregated rollup for foreach and subworkflow steps from previous runs
   */
  private WorkflowRollupOverview getAggregatedLoopAndSubworkflowStepRollup(
      String workflowId,
      long workflowInstanceId,
      Map<String, Long> stepIdRunIdLoopSubworkflowPrevious) {
    List<WorkflowRollupOverview> rollups =
        getLoopAndSubworkflowStepRollups(
            workflowId, workflowInstanceId, stepIdRunIdLoopSubworkflowPrevious);

    return rollups.stream().reduce(new WorkflowRollupOverview(), WorkflowRollupOverview::aggregate);
  }

  /**
   * Calculate list of rollups for subworkflow and foreach steps from previous runs.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param stepIdRunIdLoopSubworkflowPrevious stepId to runId map for subworkflow * and foreach
   *     steps from the previous runs
   * @return list of rollups or empty list if no foreach and subworkflow steps from prev runs
   */
  @VisibleForTesting
  List<WorkflowRollupOverview> getLoopAndSubworkflowStepRollups(
      String workflowId,
      long workflowInstanceId,
      Map<String, Long> stepIdRunIdLoopSubworkflowPrevious) {
    List<WorkflowRollupOverview> rollupOverviewsForLoopAndSubworkflow = new ArrayList<>();
    if (stepIdRunIdLoopSubworkflowPrevious == null
        || stepIdRunIdLoopSubworkflowPrevious.isEmpty()) {
      return rollupOverviewsForLoopAndSubworkflow;
    }

    List<Map<String, Artifact>> artifacts =
        stepInstanceDao.getBatchStepInstancesArtifactsFromList(
            workflowId, workflowInstanceId, stepIdRunIdLoopSubworkflowPrevious);

    for (Map<String, Artifact> artifact : artifacts) {
      if (artifact.containsKey(Artifact.Type.SUBWORKFLOW.key())) {
        SubworkflowArtifact subworkflowArtifact =
            artifact.get(Artifact.Type.SUBWORKFLOW.key()).asSubworkflow();
        if (subworkflowArtifact.getSubworkflowOverview() != null) {
          rollupOverviewsForLoopAndSubworkflow.add(
              subworkflowArtifact.getSubworkflowOverview().getRollupOverview());
        }
      }

      if (artifact.containsKey(Artifact.Type.FOREACH.key())) {
        ForeachArtifact foreachArtifact = artifact.get(Artifact.Type.FOREACH.key()).asForeach();
        if (foreachArtifact.getForeachOverview() != null
            && foreachArtifact.getForeachOverview().getCheckpoint() > 0) {
          rollupOverviewsForLoopAndSubworkflow.add(
              foreachArtifact.getForeachOverview().getOverallRollup());
        }
      }

      if (artifact.containsKey(Artifact.Type.WHILE.key())) {
        WhileArtifact whileArtifact = artifact.get(Artifact.Type.WHILE.key()).asWhile();
        if (whileArtifact != null
            && whileArtifact.getLastIteration() >= whileArtifact.getFirstIteration()) {
          rollupOverviewsForLoopAndSubworkflow.add(whileArtifact.getOverallRollup());
        }
      }
    }

    return rollupOverviewsForLoopAndSubworkflow;
  }

  /**
   * Get stepId to runId map for loop (foreach or while) and subworkflow steps from previous runs.
   *
   * @param instance current run's workflow instance
   * @return stepId to runId map for loop (foreach or while) and subworkflow steps from previous
   *     runs
   */
  @VisibleForTesting
  static Map<String, Long> getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(
      WorkflowInstance instance) {
    Map<String, StepType> stepIdToStepTypeForLoopAndSubworkflows =
        instance.getRuntimeWorkflow().getSteps().stream()
            .filter(
                step ->
                    step.getType().equals(StepType.FOREACH)
                        || step.getType().equals(StepType.WHILE)
                        || step.getType().equals(StepType.SUBWORKFLOW))
            .collect(Collectors.toMap(Step::getId, Step::getType));

    if (stepIdToStepTypeForLoopAndSubworkflows.isEmpty()) {
      // if no loop (foreach or while) and subworkflow steps in the workflow definition
      // result should be empty
      return Collections.emptyMap();
    }

    // stepIdToRunId for subworkflow and foreach and while loop steps that
    // are not part of current instance's runtimeDAG
    return instance.getAggregatedInfo().getStepAggregatedViews().entrySet().stream()
        .filter(
            step ->
                !instance.getRuntimeDag().containsKey(step.getKey())
                    && stepIdToStepTypeForLoopAndSubworkflows.containsKey(step.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, s -> s.getValue().getWorkflowRunId()));
  }

  /**
   * Get aggregated rollup for regular steps (non-subworkflow and non-foreach) from the previous
   * runs that are not included in the current run.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id
   * @param instance current runs workflow instance
   * @param foreachAndSubworkflowStepIds - set of stepIds of foreach and subworkflow steps
   * @return rollup for regular steps from previous runs
   */
  private static WorkflowRollupOverview getAggregatedRollupForStepsFromPreviousRuns(
      String workflowId,
      long workflowInstanceId,
      WorkflowInstance instance,
      Set<String> foreachAndSubworkflowStepIds) {
    return RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            workflowId,
            workflowInstanceId,
            instance.getAggregatedInfo(),
            instance.getRuntimeDag(),
            foreachAndSubworkflowStepIds)
        .stream()
        .reduce(new WorkflowRollupOverview(), WorkflowRollupOverview::aggregate);
  }

  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  @VisibleForTesting
  static List<WorkflowRollupOverview> getRollupsForStepsFromPreviousRuns(
      String workflowId,
      Long workflowInstanceId,
      WorkflowInstanceAggregatedInfo aggregatedInfo,
      Map<String, StepTransition> runtimeDag,
      Set<String> foreachAndSubworkflowStepIds) {

    List<WorkflowRollupOverview> rollupOverviews = new ArrayList<>();

    Set<Map.Entry<String, StepAggregatedView>> aggregatedStepsInfo =
        aggregatedInfo.getStepAggregatedViews().entrySet();
    for (Map.Entry<String, StepAggregatedView> entry : aggregatedStepsInfo) {
      String stepId = entry.getKey();

      if (runtimeDag.containsKey(stepId)
          || (foreachAndSubworkflowStepIds != null
              && foreachAndSubworkflowStepIds.contains(stepId))) {
        // we want to reset any steps that would've been restarted, so we don't want to add those
        // we also want to skip foreach and subworkflow steps from aggregated view because
        // we calculate rollups for those separately
        continue;
      }

      StepInstance.Status status = entry.getValue().getStatus();

      WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
      ref.setCnt(1);
      if (status.isOverview()) {
        ref.setRef(
            Collections.singletonMap(
                getReference(workflowId, entry.getValue().getWorkflowRunId()),
                Collections.singletonList(getReference(workflowInstanceId, stepId, 0L))));
      }

      rollupOverviews.add(WorkflowRollupOverview.of(1L, Collections.singletonMap(status, ref)));
    }

    return rollupOverviews;
  }

  static String getReference(String workflowId, long workflowRunId) {
    return workflowId + Constants.REFERENCE_DELIMITER + workflowRunId;
  }

  static String getReference(long workflowInstanceId, String stepId, long attemptId) {
    return workflowInstanceId
        + Constants.REFERENCE_DELIMITER
        + stepId
        + Constants.REFERENCE_DELIMITER
        + attemptId;
  }

  /**
   * Rollup base consists of subworkflow and foreach step rollups from previous runs, and regular
   * step rollups from previous runs.
   *
   * @param instance workflow instance for the current run
   * @return base rollup
   */
  public WorkflowRollupOverview calculateRollupBase(WorkflowInstance instance) {
    // if this is a fresh run no need to calculate the base
    // since all steps will be reset anyway
    // if aggregated info is not present - there would be no results
    if (instance == null
        || instance.isFreshRun()
        || instance.getRunConfig().getPolicy().equals(RunPolicy.RESTART_FROM_BEGINNING)
        || instance.getAggregatedInfo() == null
        || instance.getAggregatedInfo().getStepAggregatedViews() == null) {
      return null;
    }

    Map<String, Long> stepIdRunIdLoopSubworkflowPrevious =
        RollupAggregationHelper.getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(instance);

    WorkflowRollupOverview aggregatedBaseRollup =
        getAggregatedLoopAndSubworkflowStepRollup(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            stepIdRunIdLoopSubworkflowPrevious);

    WorkflowRollupOverview regularStepsPrevRunRollupAggregated =
        RollupAggregationHelper.getAggregatedRollupForStepsFromPreviousRuns(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance,
            stepIdRunIdLoopSubworkflowPrevious != null
                ? stepIdRunIdLoopSubworkflowPrevious.keySet()
                : null);

    if (aggregatedBaseRollup == null) {
      return regularStepsPrevRunRollupAggregated;
    }

    if (regularStepsPrevRunRollupAggregated != null) {
      aggregatedBaseRollup.aggregate(regularStepsPrevRunRollupAggregated);
    }

    return aggregatedBaseRollup;
  }
}
