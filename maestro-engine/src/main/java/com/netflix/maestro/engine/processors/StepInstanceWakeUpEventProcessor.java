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
package com.netflix.maestro.engine.processors;

import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.transformation.Translator;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.flow.engine.FlowExecutor;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.utils.Checks;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Event process to handle {@link StepInstanceWakeUpEvent}.
 *
 * <p>Note that it relies on the flowExecutor to wake up tasks. In the flowExecutor implementation,
 * it assumes all steps to wakeup are running in the single node. This is based on the fact that all
 * steps in the uber graph have the same group id.
 */
@Slf4j
@AllArgsConstructor
public class StepInstanceWakeUpEventProcessor
    implements MaestroEventProcessor<StepInstanceWakeUpEvent> {
  private final FlowExecutor flowExecutor;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;

  /**
   * Get the event and process.
   *
   * @param messageSupplier message supplier
   */
  @Override
  public void process(Supplier<StepInstanceWakeUpEvent> messageSupplier) {
    StepInstanceWakeUpEvent event = messageSupplier.get();
    if (event.getEntityType() == StepInstanceWakeUpEvent.EntityType.WORKFLOW) {
      processForWorkflowEntity(event);
    } else {
      processForStepEntity(event);
    }
  }

  /**
   * This is aiming to handle the waking up of all the leaf steps that are in retrying state when
   * user action is requested on a step, especially the nested step like foreach and subworkflow.
   */
  private void processForStepEntity(StepInstanceWakeUpEvent jobEvent) {
    // handle the simple leaf step case.
    if (jobEvent.getStepType() != null && jobEvent.getStepType().isLeaf()) {
      if (jobEvent.getStepStatus() != null && jobEvent.getStepStatus().shouldWakeup()) {
        wakeupUnderlyingTask(jobEvent);
      }
      return;
    }

    // this is a non-leaf case.
    StepInstance stepInstance =
        stepInstanceDao.getStepInstance(
            jobEvent.getWorkflowId(),
            jobEvent.getWorkflowInstanceId(),
            jobEvent.getWorkflowRunId(),
            jobEvent.getStepId(),
            jobEvent.getStepAttemptId());
    if (stepInstance == null) {
      LOG.warn(
          "Action is requested on an invalid step instance. The requested action is: {}",
          jobEvent.getMessageKey());
      return;
    }
    if (stepInstance.getDefinition().getType().isLeaf()) {
      if (stepInstance.getRuntimeState().getStatus().shouldWakeup()) {
        wakeupUnderlyingTask(jobEvent);
      }
      return;
    }

    StepInstance.Status desiredStatus =
        Checks.notNull(
                jobEvent.getStepAction(),
                "the step action cannot be null for a step action job event {}",
                jobEvent.getMessageKey())
            .getStatus();
    if (stepInstance.getRuntimeState().getStatus() == desiredStatus) {
      return;
    }
    boolean stepTerminalCheck = false;
    if (stepInstance.getArtifacts() != null) {
      switch (stepInstance.getDefinition().getType()) {
        case FOREACH:
          if (stepInstance.getArtifacts().containsKey(Artifact.Type.FOREACH.key())) {
            ForeachArtifact foreachArtifact =
                stepInstance.getArtifacts().get(Artifact.Type.FOREACH.key()).asForeach();
            handleLeafTasksWakeup(
                jobEvent.getGroupId(), foreachArtifact.getForeachOverview().getOverallRollup());
            stepTerminalCheck = desiredStatus.isTerminal();
          }
          break;
        case SUBWORKFLOW:
          if (stepInstance.getArtifacts().containsKey(Artifact.Type.SUBWORKFLOW.key())) {
            SubworkflowArtifact subworkflowArtifact =
                stepInstance.getArtifacts().get(Artifact.Type.SUBWORKFLOW.key()).asSubworkflow();
            handleLeafTasksWakeup(
                jobEvent.getGroupId(),
                subworkflowArtifact.getSubworkflowOverview().getRollupOverview());
            stepTerminalCheck = desiredStatus.isTerminal();
          }
          break;
        default:
          LOG.warn(
              "Invalid step type to be processed for this action. Action is {} and the step type is: {}",
              jobEvent.getMessageKey(),
              stepInstance.getDefinition().getType());
          return;
      }
      // need to check the desired status again in the following retry.
      if (stepTerminalCheck) {
        throw new MaestroRetryableError(
            "Current status is not the desired status after action is taking. Will check again");
      }
    }
  }

  private void wakeupUnderlyingTask(StepInstanceWakeUpEvent jobEvent) {
    String flowReference =
        String.format(
            Translator.FLOW_REFERENCE_FORMATTER,
            jobEvent.getWorkflowId(),
            jobEvent.getWorkflowInstanceId(),
            jobEvent.getWorkflowRunId());
    wakeupUnderlyingTask(jobEvent.getGroupId(), flowReference, jobEvent.getStepId());
  }

  private void wakeupUnderlyingTask(Long groupId, WorkflowRollupOverview.ReferenceEntity entity) {
    String flowReference =
        String.format(
            Translator.FLOW_REFERENCE_FORMATTER,
            entity.getWorkflowId(),
            entity.getInstanceId(),
            entity.getRunId());
    wakeupUnderlyingTask(groupId, flowReference, entity.getStepId());
  }

  private void wakeupUnderlyingTask(Long groupId, String flowReference, String stepId) {
    try {
      flowExecutor.wakeUp(groupId, flowReference, stepId);
    } catch (MaestroRuntimeException e) {
      LOG.warn("running into an exception while waking up underlying task, will try again", e);
      throw e; // retry if exception is a MaestroRetryableError
    }
  }

  /** Waking up all the leaf steps for a workflow when workflow level user action is requested. */
  private void processForWorkflowEntity(StepInstanceWakeUpEvent jobEvent) {
    WorkflowInstance workflowInstance =
        instanceDao.getWorkflowInstance(
            jobEvent.getWorkflowId(),
            jobEvent.getWorkflowInstanceId(),
            String.valueOf(jobEvent.getWorkflowRunId()),
            false);
    if (workflowInstance.getStatus()
        == Checks.notNull(
                jobEvent.getWorkflowAction(),
                "workflow action cannot be null for a workflow type job event {}",
                jobEvent)
            .getStatus()) {
      return;
    }

    handleLeafTasksWakeup(
        jobEvent.getGroupId(), workflowInstance.getRuntimeOverview().getRollupOverview());
    if (jobEvent.getWorkflowAction().getStatus().isTerminal()) {
      throw new MaestroRetryableError(
          "Current status is not the desired status after action is taking. Will check again.");
    }
  }

  private void handleLeafTasksWakeup(Long groupId, WorkflowRollupOverview overview) {
    // get all the reference entities for leaf step that has shouldWakeup flag.
    Set<WorkflowRollupOverview.ReferenceEntity> retryingLeafRefs =
        overview.getOverview().entrySet().stream()
            .filter(entry -> entry.getKey().shouldWakeup())
            .map(Map.Entry::getValue)
            .map(WorkflowRollupOverview.CountReference::getRef)
            .map(
                ref ->
                    ref.entrySet().stream()
                        .map(
                            refEntry ->
                                refEntry.getValue().stream()
                                    .map(
                                        refValue ->
                                            WorkflowRollupOverview.ReferenceEntity.decode(
                                                refEntry.getKey(), refValue))
                                    .collect(Collectors.toSet()))
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet()))
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

    // wake up the underlying internal flow tasks
    retryingLeafRefs.forEach(ref -> wakeupUnderlyingTask(groupId, ref));
  }
}
