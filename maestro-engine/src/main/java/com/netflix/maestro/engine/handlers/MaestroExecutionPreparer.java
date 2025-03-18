package com.netflix.maestro.engine.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.tracing.MaestroTracingContext;
import com.netflix.maestro.engine.transformation.Translator;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.flow.runtime.ExecutionPreparer;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.signal.SignalDependencies;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maestro execution preparer implementation based on maestro engine.
 *
 * @author jun-he
 */
public class MaestroExecutionPreparer implements ExecutionPreparer {
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final WorkflowTranslator translator;
  private final WorkflowHelper workflowHelper;
  private final RollupAggregationHelper aggregationHelper;
  private final ObjectMapper objectMapper;

  public MaestroExecutionPreparer(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      WorkflowTranslator translator,
      WorkflowHelper workflowHelper,
      RollupAggregationHelper rollupAggregationHelper,
      ObjectMapper objectMapper) {
    this.instanceDao = instanceDao;
    this.stepInstanceDao = stepInstanceDao;
    this.translator = translator;
    this.workflowHelper = workflowHelper;
    this.aggregationHelper = rollupAggregationHelper;
    this.objectMapper = objectMapper;
  }

  /**
   * For certain restart cases, it adds extra tasks (i.e. dummy tasks for param) to the flow's
   * finished tasks. It also adds all the step dependencies to the prepare task's input.
   *
   * @param flow flow to add extra tasks
   * @param task task to add the input
   */
  @Override
  public void addExtraTasksAndInput(Flow flow, Task task) {
    WorkflowSummary summary = StepHelper.retrieveWorkflowSummary(objectMapper, flow.getInput());

    if (!summary.isFreshRun()) {
      // todo this logic is currently best effort and can be improved for consecutive restarts.
      Map<String, SignalDependencies> stepDependenciesMap =
          stepInstanceDao.getAllStepDependencies(
              summary.getWorkflowId(),
              summary.getWorkflowInstanceId(),
              summary.getWorkflowRunId() - 1);
      if (stepDependenciesMap != null) {
        task.getOutputData().put(Constants.ALL_STEP_DEPENDENCIES_FIELD, stepDependenciesMap);
      }
    }

    if (!summary.isFreshRun() && summary.getRunPolicy() != RunPolicy.RESTART_FROM_BEGINNING) {
      Set<String> stepIds = new HashSet<>(summary.getStepMap().keySet());
      flow.getFlowDef().getTasks().stream()
          .flatMap(tasks -> tasks.stream().map(TaskDef::taskReferenceName))
          .forEach(stepIds::remove);
      Map<String, StepInstance> stepViews =
          stepInstanceDao.getAllLatestStepFromAncestors(
              summary.getWorkflowId(), summary.getWorkflowInstanceId(), stepIds);

      if (!stepViews.isEmpty()) {
        // build a lite taskDef for extra dummy tasks, no input for step definition.
        Map<String, TaskDef> taskDefMap =
            stepViews.values().stream()
                .collect(
                    Collectors.toMap(
                        StepInstance::getStepId,
                        si ->
                            new TaskDef(si.getStepId(), Constants.MAESTRO_TASK_NAME, null, null)));

        stepViews.values().stream()
            .map(si -> deriveTaskFromStepInstance(si, taskDefMap))
            .forEach(
                t -> {
                  // flag that the task is cloned.
                  if (t.getSeq() >= 0) {
                    t.setSeq(-(t.getSeq() + 1));
                    t.setStatus(Task.Status.SKIPPED);
                    t.setActive(false);
                  }
                  flow.addFinishedTask(t);
                });
      }
    }
  }

  /**
   * Implementation to clone a Maestro task.
   *
   * @param task task to clone
   * @return cloned task object
   */
  @Override
  public Task cloneTask(Task task) {
    Map<String, Object> outputData = task.getOutputData();
    Map<String, Object> clonedOutput = new HashMap<>();
    if (outputData.containsKey(Constants.STEP_RUNTIME_SUMMARY_FIELD)) {
      StepRuntimeSummary runtimeSummary =
          StepHelper.retrieveRuntimeSummary(objectMapper, outputData);
      StepRuntimeSummary clonedSummary =
          objectMapper.convertValue(runtimeSummary, StepRuntimeSummary.class);
      clonedOutput.put(Constants.STEP_RUNTIME_SUMMARY_FIELD, clonedSummary);
    }
    task.setOutputData(null);
    TaskDef taskDef = task.getTaskDef(); // no need to clone
    task.setTaskDef(null);
    Task cloned = objectMapper.convertValue(task, Task.class);
    cloned.setTaskDef(taskDef);
    cloned.setOutputData(clonedOutput);
    task.setTaskDef(taskDef);
    task.setOutputData(outputData);
    return cloned;
  }

  /**
   * Rebuild all transient data based on maestro engine workflow and step instance data and fill
   * them into the provided flow. It also adds extra task data to the prepare task for the restart
   * cases. It returns a flag to indicate if the prepare task should be called again to finish
   * resuming.
   *
   * @param flow flow to resume
   * @return a flag to indicate if the flow's prepare task should be called
   */
  @Override
  public boolean resume(Flow flow) {
    WorkflowInstance instance = getWorkflowInstance(flow);
    flow.setInput(
        Collections.singletonMap(
            Constants.WORKFLOW_SUMMARY_FIELD,
            workflowHelper.createWorkflowSummaryFromInstance(instance)));
    flow.setFlowDef(translator.translate(instance));
    flow.setStatus(Flow.Status.RUNNING);
    flow.setPrepareTask(flow.newTask(flow.getFlowDef().getPrepareTask(), true));
    flow.setMonitorTask(flow.newTask(flow.getFlowDef().getMonitorTask(), true));
    flow.markUpdate();

    if (instance.getRunStatus().isTerminal()) {
      flow.setStatus(Flow.Status.COMPLETED);
    } else if (instance.getExecutionId() != null) {
      loadTasks(flow, instance);
      // set prepare task's all step dependencies and also add dummy tasks to the flow
      addExtraTasksAndInput(flow, flow.getPrepareTask());
      // set prepare task start time and status
      flow.getPrepareTask()
          .setStartTime(
              instance.getStartTime() == null
                  ? System.currentTimeMillis()
                  : instance.getStartTime());
      flow.getPrepareTask().setStatus(Task.Status.COMPLETED);

      resumeMonitorTask(instance, flow.getMonitorTask());
    } else if (instance.getRunStatus() == WorkflowInstance.Status.CREATED) {
      return true;
    } else {
      throw new MaestroInvalidStatusException(
          "Cannot resume flow [%s] due to invalid status: [%s]",
          flow.getReference(), instance.getRunStatus());
    }
    return false;
  }

  private void resumeMonitorTask(WorkflowInstance instance, Task task) {
    if (instance.getRunStatus() != WorkflowInstance.Status.CREATED) {
      WorkflowRuntimeSummary runtimeSummary = new WorkflowRuntimeSummary();
      runtimeSummary.updateRuntimeState(
          WorkflowInstance.Status.IN_PROGRESS,
          instance.getRuntimeOverview(),
          instance.getStartTime());
      runtimeSummary.setRollupBase(aggregationHelper.calculateRollupBase(instance));
      runtimeSummary.setArtifacts(instance.getArtifacts());
      runtimeSummary.setTimeline(instance.getTimeline());
      task.getOutputData().put(Constants.WORKFLOW_RUNTIME_SUMMARY_FIELD, runtimeSummary);
    }
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private WorkflowInstance getWorkflowInstance(Flow flow) {
    String reference = flow.getReference();
    String[] refs = reference.split("[\\[\\]]");
    return instanceDao.getWorkflowInstanceRun(
        refs[1], Long.parseLong(refs[3]), Long.parseLong(refs[5]));
  }

  private void loadTasks(Flow flow, WorkflowInstance instance) {
    List<StepInstance> stepInstances =
        stepInstanceDao.getStepInstanceViews(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId());
    Map<String, TaskDef> taskDefMap =
        flow.getFlowDef().getTasks().stream()
            .flatMap(List::stream)
            .collect(Collectors.toMap(TaskDef::taskReferenceName, def -> def));
    long maxStepInstanceId = flow.getSeq();
    for (StepInstance stepInstance : stepInstances) {
      Task task = deriveTaskFromStepInstance(stepInstance, taskDefMap);
      if (task.isTerminal()) {
        flow.addFinishedTask(task);
      } else {
        flow.updateRunningTask(task);
      }
      maxStepInstanceId = Math.max(maxStepInstanceId, stepInstance.getStepInstanceId());
    }
    flow.setSeq(maxStepInstanceId);
  }

  private Task deriveTaskFromStepInstance(
      StepInstance stepInstance, Map<String, TaskDef> taskDefMap) {
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId(stepInstance.getStepId())
            .stepAttemptId(stepInstance.getStepAttemptId())
            .stepInstanceUuid(stepInstance.getStepUuid())
            .stepName(StepHelper.getStepNameOrDefault(stepInstance.getDefinition()))
            .stepInstanceId(stepInstance.getStepInstanceId())
            .tags(stepInstance.getTags())
            .type(stepInstance.getDefinition().getType())
            .subType(stepInstance.getDefinition().getSubType())
            .params(stepInstance.getParams())
            .transition(stepInstance.getTransition())
            .stepRetry(stepInstance.getStepRetry())
            .timeoutInMillis(
                stepInstance.getTimeoutInMillis()) // actors will re-calc timeout offset
            .synced(true)
            .runtimeState(stepInstance.getRuntimeState())
            .signalDependencies(stepInstance.getSignalDependencies())
            .signalOutputs(stepInstance.getSignalOutputs())
            .dbOperation(DbOperation.UPDATE)
            .artifacts(stepInstance.getArtifacts())
            .timeline(stepInstance.getTimeline())
            .tracingContext(getTracingContext(stepInstance))
            .stepRunParams(stepInstance.getStepRunParams())
            .restartConfig(stepInstance.getRestartConfig())
            .build();
    Task task = new Task();
    task.setTaskId(stepInstance.getStepUuid());
    task.setTaskDef(taskDefMap.get(stepInstance.getStepId()));
    task.setSeq(stepInstance.getStepInstanceId());
    task.setOutputData(new HashMap<>());
    task.getOutputData().put(Constants.STEP_RUNTIME_SUMMARY_FIELD, runtimeSummary);
    task.setStartDelayInSeconds(Translator.DEFAULT_FLOW_TASK_DELAY); // reset it to default
    task.setRetryCount(stepInstance.getStepAttemptId() - 1);
    task.setStartTime(stepInstance.getRuntimeState().getStartTime());
    task.setEndTime(stepInstance.getRuntimeState().getEndTime());
    // set status and also startDelay for certain failed states
    TaskHelper.deriveTaskStatus(task, runtimeSummary);
    return task;
  }

  private MaestroTracingContext getTracingContext(StepInstance stepInstance) {
    if (stepInstance.getArtifacts() != null) {
      var val = stepInstance.getArtifacts().get("tracing_context");
      if (val != null) {
        return objectMapper.convertValue(
            val.asDefault().getField("context"), MaestroTracingContext.class);
      }
    }
    return null;
  }
}
