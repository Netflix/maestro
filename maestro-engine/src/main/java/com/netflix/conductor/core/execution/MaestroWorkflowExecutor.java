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
package com.netflix.conductor.core.execution;

import static com.netflix.conductor.common.metadata.tasks.Task.Status.CANCELED;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.SCHEDULED;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionLockService;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.properties.MaestroConductorProperties;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Maestro customized internal workflow executor. */
@Slf4j
public class MaestroWorkflowExecutor extends WorkflowExecutor {
  private final StepRuntimeCallbackDelayPolicy stepRuntimeCallbackDelayPolicy;
  private final ObjectMapper objectMapper;
  private final ExecutionDAOFacade executionDAOFacade;
  private final QueueDAO queueDAO;
  private final DeciderService deciderService;
  private final WorkflowStatusListener workflowStatusListener;
  private final MaestroWorkflowTaskRunner taskRunner;

  /** Constructor. */
  public MaestroWorkflowExecutor(
      DeciderService deciderService,
      MetadataDAO metadataDAO,
      QueueDAO queueDAO,
      MetadataMapperService metadataMapperService,
      WorkflowStatusListener workflowStatusListener,
      ExecutionDAOFacade executionDAOFacade,
      MaestroConductorProperties props,
      ExecutionLockService executionLockService,
      ParametersUtils parametersUtils,
      StepRuntimeCallbackDelayPolicy stepRuntimeCallbackDelayPolicy,
      MaestroWorkflowTaskRunner taskRunner,
      ObjectMapper objectMapper) {
    super(
        deciderService,
        metadataDAO,
        queueDAO,
        metadataMapperService,
        workflowStatusListener,
        executionDAOFacade,
        props,
        executionLockService,
        parametersUtils);
    this.stepRuntimeCallbackDelayPolicy = stepRuntimeCallbackDelayPolicy;
    this.objectMapper = objectMapper;
    this.executionDAOFacade = executionDAOFacade;
    this.queueDAO = queueDAO;
    this.deciderService = deciderService;
    this.workflowStatusListener = workflowStatusListener;
    this.taskRunner = taskRunner;
  }

  @VisibleForTesting
  void configureCallbackInterval(Task task) {
    boolean isRunningForeachStep = false;
    boolean isMaestroTaskCreated = false;
    boolean isFirstPollingCycle =
        task != null && task.getPollCount() <= Constants.FIRST_POLLING_COUNT_LIMIT;
    if (task != null && task.getOutputData().containsKey(Constants.STEP_RUNTIME_SUMMARY_FIELD)) {
      StepRuntimeSummary runtimeSummary =
          StepHelper.retrieveRuntimeSummary(objectMapper, task.getOutputData());
      Long callbackInSecs = stepRuntimeCallbackDelayPolicy.getCallBackDelayInSecs(runtimeSummary);
      if (callbackInSecs != null) {
        LOG.trace(
            "Set customized callback [{}] in seconds for step [{}]",
            callbackInSecs,
            runtimeSummary.getIdentity());
        task.setCallbackAfterSeconds(callbackInSecs);
      }

      StepRuntimeState state = runtimeSummary.getRuntimeState();
      isRunningForeachStep =
          (runtimeSummary.getType() == StepType.FOREACH
              && state.getStatus() == StepInstance.Status.RUNNING);
      isMaestroTaskCreated = state.getStatus() == StepInstance.Status.CREATED;
      isFirstPollingCycle =
          isFirstPollingCycle
              && state.getCreateTime() != null
              && System.currentTimeMillis() - state.getCreateTime()
                  < Constants.FIRST_POLL_TIME_BUFFER_IN_MILLIS;
    }

    boolean isStartTaskStarted =
        task != null && Constants.DEFAULT_START_STEP_NAME.equals(task.getReferenceTaskName());
    // a workaround to reduce start boostrap delay for the first few polling calls
    if ((isRunningForeachStep || isMaestroTaskCreated || isStartTaskStarted)
        && isFirstPollingCycle) {
      LOG.debug(
          "Update callback to 0 for task uuid [{}] for the first few polling calls",
          task.getTaskId());
      task.setCallbackAfterSeconds(0);
    }
  }

  @VisibleForTesting
  @Override
  List<String> cancelNonTerminalTasks(Workflow workflow) {
    List<String> erroredTasks = new ArrayList<>();
    // Update non-terminal tasks' status to CANCELED
    for (Task task : workflow.getTasks()) {
      if (!task.getStatus().isTerminal()) {
        // Cancel the ones which are not completed yet....
        task.setStatus(CANCELED);
        // all of our tasks are system tasks.
        Checks.checkTrue(
            SystemTaskType.is(task.getTaskType()),
            "Invalid task type [%s], all tasks should have a known maestro task type.",
            task.getTaskType());
        WorkflowSystemTask workflowSystemTask = WorkflowSystemTask.get(task.getTaskType());
        try {
          workflowSystemTask.cancel(workflow, task, this);
          executionDAOFacade.updateTask(task); // only update if cancelled
        } catch (Exception e) {
          erroredTasks.add(task.getReferenceTaskName());
          LOG.error(
              "Error canceling system task:{}/{} in workflow: {}",
              workflowSystemTask.getName(),
              task.getTaskId(),
              workflow.getWorkflowId(),
              e);
        }
      }
    }
    if (erroredTasks.isEmpty()) {
      try {
        workflowStatusListener.onWorkflowFinalizedIfEnabled(workflow);
        queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());
      } catch (Exception e) {
        LOG.error("Error removing workflow: {} from decider queue", workflow.getWorkflowId(), e);
        throw e; // we need to throw it to get at least once guarantee.
      }
    } else {
      // also throw to retry errored tasks later.
      throw new MaestroRetryableError(
          "Error canceling tasks [%s] in workflow: [%s]", erroredTasks, workflow.getWorkflowId());
    }
    return erroredTasks;
  }

  /**
   * Optimize the conductor code to execute the async system task by removing persisting pollCount
   * to reduce DB update and also removing exceedsInProgressLimit and exceedsRateLimitPerFrequency
   * checks to improve the performance.
   */
  @Override
  public void executeSystemTask(WorkflowSystemTask systemTask, String taskId, int callbackTime) {
    try {
      Task task = executionDAOFacade.getTaskById(taskId);
      if (task == null) {
        LOG.error("TaskId: {} could not be found while executing SystemTask", taskId);
        return;
      }
      LOG.debug("Task: {} fetched from execution DAO for taskId: {}", task, taskId);
      String queueName = QueueUtils.getQueueName(task);
      if (task.getStatus().isTerminal()) {
        // Tune the SystemTaskWorkerCoordinator's queues - if the queue size is very big this can
        // happen!
        LOG.info("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
        queueDAO.remove(queueName, task.getTaskId());
        return;
      }

      String workflowId = task.getWorkflowInstanceId();
      Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);

      if (task.getStartTime() == 0) {
        task.setStartTime(System.currentTimeMillis());
        executionDAOFacade.updateTask(task);
        Monitors.recordQueueWaitTime(task.getTaskDefName(), task.getQueueWaitTime());
      }

      if (workflow.getStatus().isTerminal()) {
        LOG.info(
            "Workflow {} has been completed for {}/{}",
            workflow.getWorkflowId(),
            systemTask.getName(),
            task.getTaskId());
        if (!task.getStatus().isTerminal()) {
          task.setStatus(CANCELED);
        }
        executionDAOFacade.updateTask(task);
        queueDAO.remove(queueName, task.getTaskId());
        return;
      }

      LOG.debug("Executing {}/{}-{}", task.getTaskType(), task.getTaskId(), task.getStatus());
      if (task.getStatus() == SCHEDULED || !systemTask.isAsyncComplete(task)) {
        task.setPollCount(task.getPollCount() + 1);
        // removed poll count DB update here
      }

      deciderService.populateTaskData(task);

      // Stop polling for asyncComplete system tasks that are not in SCHEDULED state
      if (systemTask.isAsyncComplete(task) && task.getStatus() != SCHEDULED) {
        queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
        return;
      }

      taskRunner.runMaestroTask(this, workflow, task, systemTask);

      if (!task.getStatus().isTerminal()) {
        task.setCallbackAfterSeconds(callbackTime);
        try {
          configureCallbackInterval(task); // overwrite if needed
        } catch (Exception e) {
          LOG.error(
              "Error configuring callback interval for task [{}]. Please investigate it",
              task.getTaskId(),
              e);
        }
      }

      updateTask(new TaskResult(task));
      LOG.debug(
          "Done Executing {}/{}-{} output={}",
          task.getTaskType(),
          task.getTaskId(),
          task.getStatus(),
          task.getOutputData());

    } catch (Exception e) {
      Monitors.error("MaestroWorkflowExecutor", "executeSystemTask");
      LOG.error("Error executing system task - {}, with id: {}", systemTask, taskId, e);
    }
  }

  public boolean requeueSweep(String workflowId) {
    return queueDAO.pushIfNotExists(WorkflowExecutor.DECIDER_QUEUE, workflowId, 0L);
  }

  /**
   * Reset a retrying task delay time given by previously failed task id.
   *
   * @param previousFailedTaskId the identifier of the previously failed task.
   * @return if the operation is successful.
   */
  public boolean resetTaskOffset(String previousFailedTaskId) {
    final Task task = executionDAOFacade.getTaskById(previousFailedTaskId);
    final Workflow workflow =
        executionDAOFacade.getWorkflowById(task.getWorkflowInstanceId(), true);
    Optional<Task> currentTask =
        workflow.getTasks().stream()
            .filter(t -> task.getTaskId().equals(t.getRetriedTaskId()))
            .findFirst();
    if (currentTask.isPresent()) {
      final Task enqueuedTask = currentTask.get();
      return queueDAO.resetOffsetTime(
          QueueUtils.getQueueName(enqueuedTask), enqueuedTask.getTaskId());
    }
    return false;
  }
}
