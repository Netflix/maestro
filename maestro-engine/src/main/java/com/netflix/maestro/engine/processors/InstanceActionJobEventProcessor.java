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
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.IdHelper;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Event process to handle {@link InstanceActionJobEvent}.
 *
 * <p>Note that it relies on the flowExecutor to wake up tasks. In the flowExecutor implementation,
 * it assumes all steps of a single workflow instance to wakeup are running in the single node. But
 * the steps from the uber graph (e.g., subworkflow or foreach) can have a different group id.
 */
@Slf4j
@AllArgsConstructor
public class InstanceActionJobEventProcessor
    implements MaestroEventProcessor<InstanceActionJobEvent> {
  private final FlowOperation flowOperation;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;

  /**
   * Get the event and process.
   *
   * @param jobEvent message to process
   */
  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  @Override
  public Optional<MaestroJobEvent> process(InstanceActionJobEvent jobEvent) {
    switch (jobEvent.getEntityType()) {
      case WORKFLOW -> processForWorkflowEntity(jobEvent);
      case STEP -> processForStepEntity(jobEvent);
      case FLOW -> processForFlowEntity(jobEvent);
    }
    return Optional.empty();
  }

  /**
   * Wakeup the steps of the workflow instance specified in the job event. It also checks the
   * workflow instance in DB to see if the action is taken.
   */
  private void processForWorkflowEntity(InstanceActionJobEvent jobEvent) {
    WorkflowInstance workflowInstance =
        instanceDao.getWorkflowInstanceRun(
            jobEvent.getWorkflowId(),
            jobEvent.getWorkflowInstanceId(),
            jobEvent.getWorkflowRunId());
    if (workflowInstance.getStatus()
        == Checks.notNull(
                jobEvent.getWorkflowAction(),
                "workflow action cannot be null for a workflow type job event {}",
                jobEvent)
            .getStatus()) {
      return;
    }
    if (workflowInstance.getStatus() != null && workflowInstance.getStatus().isTerminal()) {
      LOG.info(
          "workflow instance [{}] status [{}] is terminal and no need to wake up any entity.",
          workflowInstance.getIdentity(),
          workflowInstance.getStatus());
      return;
    }

    wakeUpUnderlyingActor(jobEvent);

    if (jobEvent.getWorkflowAction().getStatus().isTerminal()) {
      throw new MaestroRetryableError(
          "Current status [%s] is not the desired status after action is taking. Will check again.",
          workflowInstance.getStatus());
    }
  }

  /**
   * This is aiming to handle the waking up of a step specified in the job event. It also checks if
   * the action is applied to the step. For the nested step like foreach and subworkflow, the step
   * runtime will wake up its downstream steps directly.
   */
  private void processForStepEntity(InstanceActionJobEvent jobEvent) {
    if (jobEvent.getStepAction() == Actions.StepInstanceAction.RESTART
        || jobEvent.getStepAction() == Actions.StepInstanceAction.BYPASS_STEP_DEPENDENCIES) {
      wakeUpUnderlyingActor(jobEvent);
      return;
    }

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
          jobEvent.getIdentity());
      return;
    }
    if (!stepInstance.getRuntimeState().getStatus().shouldWakeup()) {
      return; // done
    }
    wakeUpUnderlyingActor(jobEvent);
    throw new MaestroRetryableError(
        "Current step status is not the desired status after action is taking. Will check again");
  }

  private void wakeUpUnderlyingActor(InstanceActionJobEvent jobEvent) {
    String flowRef =
        IdHelper.deriveFlowRef(
            jobEvent.getWorkflowId(),
            jobEvent.getWorkflowInstanceId(),
            jobEvent.getWorkflowRunId());
    long groupId = IdHelper.deriveGroupId(flowRef, jobEvent.getGroupInfo());
    try {
      boolean done =
          jobEvent.getStepId() == null
              ? flowOperation.wakeUp(groupId, Set.of(flowRef))
              : flowOperation.wakeUp(groupId, flowRef, jobEvent.getStepId());
      if (!done) {
        throw new MaestroRetryableError(
            "The underlying task [%s] in flow [%s] for group [%s] is not woken up successfully. Will try again.",
            jobEvent.getStepId(), flowRef, groupId);
      }
    } catch (MaestroRuntimeException e) {
      LOG.info(
          "running into an exception while waking up underlying task [{}][{}][{}], will try again",
          groupId,
          flowRef,
          jobEvent.getStepId());
      throw e; // retry if exception is a MaestroRetryableError
    }
  }

  // Best effort wakeup the actors. It ignores any error or if wakeup is not done.
  private void processForFlowEntity(InstanceActionJobEvent jobEvent) {
    String workflowId = jobEvent.getWorkflowId();
    long groupInfo = jobEvent.getGroupInfo();
    var groupedRefs = new HashMap<Long, Set<String>>();
    for (var entry : jobEvent.getInstanceRunIds().entrySet()) {
      String flowRef = IdHelper.deriveFlowRef(workflowId, entry.getKey(), entry.getValue());
      long groupId = IdHelper.deriveGroupId(flowRef, groupInfo);
      if (!groupedRefs.containsKey(groupId)) {
        groupedRefs.put(groupId, new HashSet<>());
      }
      groupedRefs.get(groupId).add(flowRef);
    }

    // todo if too many groups, we can use parallel() to speed up
    groupedRefs.forEach(
        (groupId, refs) -> {
          try {
            flowOperation.wakeUp(groupId, refs); // if not done or failed, no retry here.
          } catch (MaestroRuntimeException e) {
            LOG.info(
                "running into an exception while waking up underlying flows [{}][{}], will ignore it",
                groupId,
                refs);
          }
        });
  }
}
