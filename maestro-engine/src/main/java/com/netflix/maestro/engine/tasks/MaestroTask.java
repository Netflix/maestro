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
package com.netflix.maestro.engine.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.concurrency.TagPermitManager;
import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.eval.InstanceWrapper;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeManager;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.StepSyncManager;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.SignalHandler;
import com.netflix.maestro.engine.metrics.MaestroMetrics;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.tracing.MaestroTracingContext;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.engine.transformation.Translator;
import com.netflix.maestro.engine.utils.DagHelper;
import com.netflix.maestro.engine.utils.DurationHelper;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.FailureMode;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepDependenciesDefinition;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepDependencies;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstanceTransition;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.DurationParser;
import com.netflix.maestro.utils.MapHelper;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro task implementation, which is a proxy to bridge maestro and conductor.
 *
 * <p>It is responsible to retrieve Maestro data model from conductor data and pass it to Maestro
 * step runtime.
 *
 * <p>It encapsulates conductor engine execution model. Thus, all maestro step runtime will be
 * independent of the conductor.
 *
 * <p>It also handles the at-least once step status change publishing and the maestro step instance
 * data update and persistence.
 */
@Slf4j
public class MaestroTask extends WorkflowSystemTask {
  private static final Set<String> RETRYABLE_SQL_ERROR_STATES = Collections.singleton("08006");
  private static final Set<String> RETRYABLE_SQL_ERROR_MSGS =
      Collections.singleton("Connection is closed");

  private final StepRuntimeManager stepRuntimeManager;
  private final StepSyncManager stepSyncManager;
  private final ParamEvaluator paramEvaluator;
  private final ObjectMapper objectMapper;
  private final SignalHandler signalHandler;
  private final OutputDataManager outputDataManager;
  private final MaestroStepBreakpointDao stepBreakpointDao;
  private final MaestroStepInstanceActionDao actionDao;
  private final TagPermitManager tagPermitAcquirer;
  private final InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;
  private final MaestroMetrics metrics;
  private final MaestroTracingManager tracingManager;
  private final MaestroParamExtensionRepo paramExtensionRepo;

  /** Maestro task constructor. */
  public MaestroTask(
      StepRuntimeManager stepRuntimeManager,
      StepSyncManager stepSyncManager,
      ParamEvaluator paramEvaluator,
      ObjectMapper objectMapper,
      SignalHandler signalClient,
      OutputDataManager outputDataManager,
      MaestroStepBreakpointDao stepBreakpointDao,
      MaestroStepInstanceActionDao actionDao,
      TagPermitManager tagPermitAcquirer,
      InstanceStepConcurrencyHandler instanceStepConcurrencyHandler,
      MaestroMetrics metricRepo,
      @Nullable MaestroTracingManager tracingManager,
      @Nullable MaestroParamExtensionRepo extensionRepo) {
    super(Constants.MAESTRO_TASK_NAME);
    this.stepRuntimeManager = stepRuntimeManager;
    this.stepSyncManager = stepSyncManager;
    this.paramEvaluator = paramEvaluator;
    this.objectMapper = objectMapper;
    this.signalHandler = signalClient;
    this.outputDataManager = outputDataManager;
    this.stepBreakpointDao = stepBreakpointDao;
    this.actionDao = actionDao;
    this.tagPermitAcquirer = tagPermitAcquirer;
    this.instanceStepConcurrencyHandler = instanceStepConcurrencyHandler;
    this.metrics = metricRepo;
    this.tracingManager = tracingManager;
    if (tracingManager == null) {
      LOG.info("Tracing manager is null, all tracing will be skipped.");
    }
    this.paramExtensionRepo = extensionRepo;
  }

  @Override
  public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
    try {
      Step stepDefinition = StepHelper.retrieveStepDefinition(objectMapper, task.getInputData());
      WorkflowSummary workflowSummary =
          StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());
      Map<StepDependencyType, StepDependencies> dependencies =
          StepHelper.getStepDependencies(workflow, stepDefinition.getId(), objectMapper);
      StepRuntimeSummary runtimeSummary =
          createStepRuntimeSummary(task, stepDefinition, workflowSummary, dependencies);
      task.getOutputData().put(Constants.STEP_RUNTIME_SUMMARY_FIELD, runtimeSummary);
      task.setStartDelayInSeconds(Translator.CONDUCTOR_RETRY_DELAY); // reset it to default
      LOG.info(
          "Created a step instance {} for workflow instance [{}] with status [{}]",
          runtimeSummary.getIdentity(),
          workflow.getWorkflowName(),
          runtimeSummary.getRuntimeState().getStatus().name());

      if (stepDefinition.getType().isLeaf()
          && workflowSummary.getInstanceStepConcurrency() != null // if enabled
          && runtimeSummary.getRuntimeState().getStatus() == StepInstance.Status.CREATED) {
        Optional<Details> result =
            instanceStepConcurrencyHandler.registerStep(
                workflowSummary.getCorrelationId(), runtimeSummary.getStepInstanceUuid());
        result.ifPresent(
            details ->
                LOG.warn(
                    "Failed to register the step but will still continue the execution due to [{}]",
                    details));
      }
    } catch (Exception e) {
      handleUnexpectedException(workflow, task, e);
    }
  }

  private StepRuntimeSummary createStepRuntimeSummary(
      Task task,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      Map<StepDependencyType, StepDependencies> dependencies) {

    StepInstance.StepRetry stepRetry;
    long stepInstanceId;
    Map<StepDependencyType, StepDependencies> dependenciesToUse;
    if (task.getRetryCount() == 0) { // this is a new start
      stepRetry = StepInstance.StepRetry.from(stepDefinition.getRetryPolicy());
      stepInstanceId = task.getSeq(); // may have a gap but increasing monotonically
      dependenciesToUse = dependencies;

      // handle sequential restart cases with dummy root nodes (NOT_CREATED)
      if (!workflowSummary.isFreshRun()
          && workflowSummary.getRestartConfig() != null
          && workflowSummary.getRunPolicy() == RunPolicy.RESTART_FROM_SPECIFIC) {
        Set<String> dummyRootStepIds =
            DagHelper.getNotCreatedRootNodesInRestartRuntimeDag(
                workflowSummary.getRuntimeDag(), workflowSummary.getRestartConfig());
        if (dummyRootStepIds.contains(stepDefinition.getId())) {
          stepRetry.setRetryable(false);
        }
      }
    } else { // this is a retry
      StepRuntimeSummary prev =
          StepHelper.retrieveRuntimeSummary(objectMapper, task.getOutputData());
      stepRetry = prev.getStepRetry();
      stepRetry.incrementByStatus(prev.getRuntimeState().getStatus());
      stepInstanceId = prev.getStepInstanceId();
      dependenciesToUse = prev.getDependencies();
    }

    long stepAttemptId = task.getRetryCount() + 1L;
    MaestroTracingContext tracingContext = null;
    if (tracingManager != null) {
      tracingContext =
          tracingManager.initTracingContext(
              workflowSummary,
              StepRuntimeSummary.builder()
                  .stepId(stepDefinition.getId())
                  .stepInstanceId(stepInstanceId)
                  .stepAttemptId(stepAttemptId)
                  .stepInstanceUuid(task.getTaskId())
                  .build());
    }

    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId(stepDefinition.getId())
            .stepAttemptId(stepAttemptId)
            .stepInstanceUuid(task.getTaskId())
            .stepName(StepHelper.getStepNameOrDefault(stepDefinition))
            .stepInstanceId(stepInstanceId)
            .tags(stepDefinition.getTags())
            .type(stepDefinition.getType())
            .subType(stepDefinition.getSubType())
            .params(new LinkedHashMap<>()) // empty placeholder
            .transition(StepInstanceTransition.from(stepDefinition))
            .stepRetry(stepRetry)
            .timeoutInMillis(null) // mean to use system default timeout initially
            .synced(true)
            .dependencies(dependenciesToUse)
            .dbOperation(DbOperation.INSERT)
            .tracingContext(tracingContext)
            .artifacts(
                Optional.ofNullable(tracingContext)
                    .map(MaestroTracingContext::toTracingArtifacts)
                    .orElse(null))
            .build();

    if (!stepRetry.isRetryable()) {
      LOG.debug(
          "Create a placeholder task for workflow {}{} with failure model [{}]",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          stepDefinition.getFailureMode());
      runtimeSummary.getRuntimeState().setStatus(StepInstance.Status.NOT_CREATED);
    } else {
      runtimeSummary.markCreated(tracingManager);
    }
    return runtimeSummary;
  }

  /**
   * Unexpected failure due to runtime data deserialization and no retry. It is likely due to a bug.
   * Step itself won't be able to handle it as the step runtime summary may not exist. Rely on the
   * final workflow level termination to record this kind of error. Error info will be logged in
   * maestro workflow instance db.
   */
  private void handleUnexpectedException(Workflow workflow, Task task, Exception e) {
    task.setStatus(Task.Status.FAILED_WITH_TERMINAL_ERROR);
    task.setReasonForIncompletion(
        String.format(
            "Step [%s] got an unexpected exception: %s",
            task.getReferenceTaskName(), e.getMessage()));
    LOG.error(
        "Terminate Maestro step [{}] for the task [{}] in workflow [{}][{}], getting an exception",
        task.getReferenceTaskName(),
        task.getTaskId(),
        workflow.getWorkflowName(),
        workflow.getWorkflowId(),
        e);
    metrics.counter(
        "handle_unexpected_exception_in_maestro_task",
        getClass(),
        "exception",
        e.getClass().getSimpleName());
  }

  private boolean initializeAndSendOutputSignals(
      Workflow workflow,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary) {
    try {
      Map<String, Map<String, Object>> allStepOutputData =
          TaskHelper.getAllStepOutputData(workflow);

      initializeOutputSignals(allStepOutputData, stepDefinition, workflowSummary, runtimeSummary);

      return runtimeSummary.getOutputs() == null
          || signalHandler.sendOutputSignals(workflowSummary, runtimeSummary);
    } catch (Exception e) {
      LOG.error(
          "Failed to evaluate Maestro params for output signals for {}{} due to error:",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      throw e;
    }
  }

  private boolean initialize(
      Workflow workflow,
      Task task,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary) {
    try {
      Map<String, Map<String, Object>> allStepOutputData =
          TaskHelper.getAllStepOutputData(workflow);

      if (isStepSatisfied(allStepOutputData, runtimeSummary)
          && !isStepSkipped(workflowSummary, runtimeSummary)
          && isStepEnabled()) {
        initializeStepRuntime(allStepOutputData, stepDefinition, workflowSummary, runtimeSummary);
        return true;
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to initialize Maestro step for the task [{}] in workflow [{}][{}], get an exception:",
          task.getTaskId(),
          workflow.getWorkflowName(),
          workflow.getWorkflowId(),
          e);
      runtimeSummary.markInternalError(e, tracingManager);
    }
    return false;
  }

  private boolean evaluateParams(
      Workflow workflow,
      Task task,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary) {
    try {
      Map<String, Map<String, Object>> allStepOutputData =
          TaskHelper.getAllStepOutputData(workflow);

      Map<String, Parameter> allStepParams =
          stepRuntimeManager.getAllParams(stepDefinition, workflowSummary, runtimeSummary);

      Map<String, List<Map<String, Parameter>>> signalDependenciesParams =
          signalHandler.getDependenciesParams(runtimeSummary);

      // First, only support step param evaluation with param extension.
      paramExtensionRepo.reset(
          allStepOutputData,
          signalDependenciesParams,
          InstanceWrapper.from(workflowSummary, runtimeSummary));
      paramEvaluator.evaluateStepParameters(
          allStepOutputData,
          workflowSummary.getParams(),
          allStepParams,
          runtimeSummary.getStepId());
      paramExtensionRepo.clear();

      runtimeSummary.mergeParams(allStepParams);
      runtimeSummary.markWaitPermit(tracingManager);
      return true;
    } catch (MaestroRetryableError mre) {
      throw mre;
    } catch (Exception e) {
      LOG.warn(
          "Failed to evaluate Maestro step params for the task [{}] in workflow [{}][{}], get an exception:",
          task.getTaskId(),
          workflow.getWorkflowName(),
          workflow.getWorkflowId(),
          e);
      runtimeSummary.markInternalError(e, tracingManager);
      return false;
    }
  }

  private void initializeOutputSignals(
      Map<String, Map<String, Object>> allStepOutputData,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary) {
    Map<String, Parameter> allStepParams = runtimeSummary.getParams();
    Map<StepOutputsDefinition.StepOutputType, StepOutputsDefinition> stepOutputs =
        stepDefinition.getOutputs();
    if (stepOutputs != null) {
      Map<StepOutputsDefinition.StepOutputType, List<MapParameter>> stepOutputsParameters =
          ParamsManager.getStepOutputsParameters(stepOutputs.values());
      if (!stepOutputsParameters.isEmpty()) {
        paramEvaluator.evaluateStepDependenciesOrOutputsParameters(
            allStepOutputData,
            workflowSummary.getParams(),
            allStepParams,
            stepOutputsParameters.values(),
            runtimeSummary.getStepId());
        runtimeSummary.initializeOutputs(stepOutputsParameters);
      }
    }
  }

  private void emitStepDelayMetric(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getRuntimeState() == null) {
      return;
    }

    Long stepInitTime = runtimeSummary.getRuntimeState().getInitializeTime();
    Long stepCreateTime = runtimeSummary.getRuntimeState().getCreateTime();

    if (stepInitTime == null || stepCreateTime == null) {
      return;
    }

    long stepInitDelay = stepInitTime - stepCreateTime;
    metrics.timer(MetricConstants.STEP_INITIALIZE_DELAY_METRIC, stepInitDelay, getClass());
  }

  private void initializeTimeout(
      Step stepDefinition, WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    if (stepDefinition.getTimeout() != null) {
      Long timeout =
          DurationParser.getDurationWithParamInMillis(
              stepDefinition.getTimeout(),
              p ->
                  paramEvaluator.parseAttribute(
                      p, workflowSummary.getParams(), workflowSummary.getIdentity(), false));
      runtimeSummary.setTimeoutInMillis(timeout);
    }
  }

  private void initializeStepRuntime(
      Map<String, Map<String, Object>> allStepOutputData,
      Step stepDefinition,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary) {
    initializeTimeout(stepDefinition, workflowSummary, runtimeSummary);

    if (runtimeSummary.getDependencies() == null && stepDefinition.getDependencies() != null) {
      Map<StepDependencyType, StepDependenciesDefinition> dependencies =
          stepDefinition.getDependencies();
      Map<StepDependencyType, List<MapParameter>> stepDependenciesParameters =
          ParamsManager.getStepDependenciesParameters(dependencies.values());
      if (!stepDependenciesParameters.isEmpty()) {
        paramEvaluator.evaluateStepDependenciesOrOutputsParameters(
            allStepOutputData,
            workflowSummary.getParams(),
            Collections.emptyMap(), // dependencies cannot use its own step params
            stepDependenciesParameters.values(),
            runtimeSummary.getStepId());
        runtimeSummary.initializeStepDependenciesSummaries(stepDependenciesParameters);
      }
    }

    runtimeSummary.mergeTags(stepRuntimeManager.getRuntimeTags(stepDefinition));
    runtimeSummary.markInitialized(tracingManager);
    emitStepDelayMetric(runtimeSummary);
  }

  /**
   * Check if step is enabled. Currently, it is always true.
   *
   * <p>todo: check if the step is enabled when disable action is supported.
   *
   * @return if the step is enabled
   */
  private boolean isStepEnabled() {
    return true;
  }

  @VisibleForTesting
  boolean isStepSkipped(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    RestartConfig stepRestartConfig =
        ObjectHelper.valueOrDefault(
            runtimeSummary.getRestartConfig(), workflowSummary.getRestartConfig());
    if (stepRestartConfig != null
        && stepRestartConfig.getRestartPath() != null
        && stepRestartConfig.getRestartPath().size() == 1
        && stepRestartConfig.getSkipSteps() != null) {
      boolean skipped = stepRestartConfig.getSkipSteps().contains(runtimeSummary.getStepId());
      if (skipped) {
        LOG.info(
            "workflow {}'s step {} is skipped.",
            workflowSummary.getIdentity(),
            runtimeSummary.getIdentity());
        runtimeSummary.markTerminated(StepInstance.Status.SKIPPED, tracingManager);
        runtimeSummary.addTimeline(
            TimelineLogEvent.info("Step is skipped because of a user skip action."));
      }
      return skipped;
    }
    return false;
  }

  private boolean isStepSatisfied(
      Map<String, Map<String, Object>> allStepOutputData, StepRuntimeSummary runtimeSummary) {

    List<String> predecessors = runtimeSummary.getTransition().getPredecessors();
    boolean satisfied =
        predecessors.isEmpty()
            || predecessors.stream()
                .anyMatch(
                    predecessor -> {
                      StepInstanceTransition transition =
                          StepHelper.retrieveStepTransition(
                              objectMapper,
                              allStepOutputData.getOrDefault(predecessor, Collections.emptyMap()));
                      return transition.getSuccessors().get(runtimeSummary.getStepId()).asBoolean();
                    });
    if (!satisfied) {
      runtimeSummary.addTimeline(
          TimelineLogEvent.info(
              "Step condition is unsatisfied because no predecessors can reach it."));
    }

    runtimeSummary.mergeParams(
        Collections.singletonMap(
            Constants.STEP_SATISFIED_FIELD,
            BooleanParameter.builder()
                .value(satisfied)
                .evaluatedResult(satisfied)
                .evaluatedTime(System.currentTimeMillis())
                .build()));

    if (!satisfied) {
      runtimeSummary.markTerminated(StepInstance.Status.UNSATISFIED, tracingManager);
    }
    return satisfied;
  }

  /** Check if the execution is timed out, which is based on step start time. */
  private boolean isTimeout(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getRuntimeState() != null
        && runtimeSummary.getRuntimeState().getStartTime() != null) {
      if (runtimeSummary.getRuntimeState().getStatus() == StepInstance.Status.TIMED_OUT
          || runtimeSummary.getRuntimeState().getStatus() == StepInstance.Status.TIMEOUT_FAILED) {
        return true;
      }
      long timeoutInMillis =
          ObjectHelper.valueOrDefault(
              runtimeSummary.getTimeoutInMillis(), Defaults.DEFAULT_TIME_OUT_LIMIT_IN_MILLIS);
      return System.currentTimeMillis() - runtimeSummary.getRuntimeState().getStartTime()
          >= timeoutInMillis;
    }
    return false;
  }

  /** If there is an action, update runtime data based on the action. */
  private void tryUpdateByAction(
      WorkflowSummary workflowSummary, Step stepDefinition, StepRuntimeSummary runtimeSummary) {
    StepInstance.Status status = runtimeSummary.getRuntimeState().getStatus();
    if (status.isTerminal()) {
      return;
    }
    Optional<StepAction> stepAction =
        actionDao.tryGetAction(workflowSummary, stepDefinition.getId());
    stepAction.ifPresent(
        action -> {
          // add this special check because a task in a non-skippable status might be waked up
          if (action.getAction() == Actions.StepInstanceAction.SKIP
              && !Actions.STEP_INSTANCE_STATUS_TO_ACTION_MAP
                  .getOrDefault(status, Collections.emptyList())
                  .contains(action.getAction())) {
            LOG.info(
                "Workflow instance {} get an action [{}] unsupported by action map on the step {}, ignore it.",
                workflowSummary.getIdentity(),
                action.getAction(),
                runtimeSummary.getIdentity());
            return; // cannot take this action in the current status
          }

          // not check STEP_INSTANCE_STATUS_TO_ACTION_MAP as the action should be already validated
          switch (action.getAction()) {
            case RESTART:
              if (status == StepInstance.Status.NOT_CREATED) {
                runtimeSummary.setStepRunParams(action.getRunParams());
                runtimeSummary.setRestartConfig(action.getRestartConfig());
                runtimeSummary.markCreated(tracingManager);
                break;
              } else {
                if (status == StepInstance.Status.RUNNING) {
                  runtimeSummary.setPendingAction(action);
                }
                return;
              }
            case KILL: // mark the step fatally failed, kill job
              runtimeSummary.configIgnoreFailureMode(action, workflowSummary);
              // fall through
            case STOP: // mark the step stopped, kill job
            case SKIP: // mark the step skipped, terminate job and then continue with next step
              if (status != StepInstance.Status.NOT_CREATED) {
                // might throw retryable error if it is still terminating.
                terminate(
                    workflowSummary,
                    runtimeSummary,
                    action.getTerminalStatus(
                        workflowSummary.getWorkflowId(), workflowSummary.getWorkflowInstanceId()));
                break;
              } else {
                return;
              }
            case BYPASS_STEP_DEPENDENCIES:
              if (status != StepInstance.Status.WAITING_FOR_SIGNALS) {
                LOG.info("Ignore bypass dependency action as current status is: {}", status);
              } else {
                runtimeSummary.byPassStepDependencies(action.getUser(), action.getCreateTime());
                // skip adding the timeline info for action as its already taken care in the
                // byPassStepDependencies.
              }
              return;
            default:
              LOG.info("Ignore unknown action: {}", action);
              return;
          }
          LOG.info(
              "Workflow instance {} take an action [{}] on the step {}",
              workflowSummary.getIdentity(),
              action.getAction(),
              runtimeSummary.getIdentity());
          runtimeSummary.addTimeline(action.toTimelineEvent());
        });
  }

  @Override
  public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
    try {
      WorkflowSummary workflowSummary =
          StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());
      Step stepDefinition = StepHelper.retrieveStepDefinition(objectMapper, task.getInputData());
      StepRuntimeSummary runtimeSummary =
          StepHelper.retrieveRuntimeSummary(objectMapper, task.getOutputData());

      LOG.info(
          "Execute a step instance {} for workflow instance {} with status [{}]",
          runtimeSummary.getIdentity(),
          workflowSummary.getIdentity(),
          runtimeSummary.getRuntimeState().getStatus().name());

      if (isTimeout(runtimeSummary)) {
        handleTimeoutError(workflowSummary, runtimeSummary);
      } else {
        tryUpdateByAction(workflowSummary, stepDefinition, runtimeSummary);
      }

      if (doExecute(workflow, task, workflowSummary, stepDefinition, runtimeSummary)) {
        return false;
      }

      updateRetryDelayTimeToTimeline(runtimeSummary);

      if (runtimeSummary.isSynced()) {
        return false;
      } else {
        syncPendingUpdates(task, workflowSummary, runtimeSummary, false);
        return true;
      }
    } catch (MaestroRetryableError mre) { // will retry in the next polling cycle
      return false;
    } catch (Exception e) {
      if (isRetryableError(e)) {
        LOG.warn(
            "Caught a SQLException for workflow [{}] and will retry", workflow.getWorkflowId(), e);
        return false;
      }
      handleUnexpectedException(workflow, task, e);
      return true; // swallow exception and fail the workflow
    }
  }

  private void handleTimeoutError(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    LOG.info(
        "Workflow instance {}'s step {} is timed out.",
        workflowSummary.getIdentity(),
        runtimeSummary.getIdentity());
    if (runtimeSummary.getStepRetry().hasReachedTimeoutRetryLimit()) {
      terminate(workflowSummary, runtimeSummary, StepInstance.Status.TIMED_OUT);
      runtimeSummary.addTimeline(TimelineLogEvent.info("Step instance is timed out."));
    } else {
      runtimeSummary.markTerminated(StepInstance.Status.TIMEOUT_FAILED, tracingManager);
    }
  }

  // Figure out if an exception is retryable
  private boolean isRetryableError(Exception e) {
    if (e.getCause() instanceof SQLException) {
      SQLException sqlException = (SQLException) e.getCause();
      return (sqlException.getSQLState() != null
              && RETRYABLE_SQL_ERROR_STATES.contains(sqlException.getSQLState()))
          || (sqlException.getSQLState() == null
              && RETRYABLE_SQL_ERROR_MSGS.contains(sqlException.getMessage()));
    }
    return false;
  }

  /**
   * If a step is in first polling, the state machine is short circuit to move forward as much as
   * possible to save the bootstrap delays.
   */
  private boolean isFirstPolling(Task task, StepRuntimeSummary runtimeSummary) {
    StepRuntimeState state = runtimeSummary.getRuntimeState();
    boolean isFirst =
        (runtimeSummary.getType() == StepType.SUBWORKFLOW
                || runtimeSummary.getType() == StepType.FOREACH)
            && state.getStatus() == StepInstance.Status.CREATED
            && state.getCreateTime() != null
            && System.currentTimeMillis() - state.getCreateTime()
                < Constants.FIRST_POLL_TIME_BUFFER_IN_MILLIS
            && task.getPollCount() <= Constants.FIRST_POLLING_COUNT_LIMIT;
    if (isFirst) {
      LOG.info(
          "Execute a step instance {} for the first time, which has a status [{}]",
          runtimeSummary.getIdentity(),
          state.getStatus().name());
    }
    return isFirst;
  }

  /** Executes the step instance. It returns true, if the task is in dummy run mode. */
  private boolean doExecute(
      Workflow workflow,
      Task task,
      WorkflowSummary workflowSummary,
      Step stepDefinition,
      StepRuntimeSummary runtimeSummary) {
    boolean doneWithExecute = false;
    boolean isFirstPolling = isFirstPolling(task, runtimeSummary);
    while (!doneWithExecute) {
      try {
        switch (runtimeSummary.getRuntimeState().getStatus()) {
          case NOT_CREATED:
            // this is for internal placeholder task to keep the state non-terminal
            return true;
          case CREATED:
            doneWithExecute =
                initialize(workflow, task, stepDefinition, workflowSummary, runtimeSummary);
            doneWithExecute = !isFirstPolling && doneWithExecute;
            break;
          case INITIALIZED:
            if (stepBreakpointDao.createPausedStepAttemptIfNeeded(
                workflowSummary.getWorkflowId(),
                workflowSummary.getWorkflowVersionId(),
                workflowSummary.getWorkflowInstanceId(),
                workflowSummary.getWorkflowRunId(),
                runtimeSummary.getStepId(),
                runtimeSummary.getStepAttemptId())) {
              runtimeSummary.markPaused(tracingManager);
            } else {
              runtimeSummary.markWaitSignal(tracingManager);
            }
            break;
          case PAUSED:
            if (stepBreakpointDao.shouldStepResume(
                workflowSummary.getWorkflowId(),
                workflowSummary.getWorkflowVersionId(),
                workflowSummary.getWorkflowInstanceId(),
                workflowSummary.getWorkflowRunId(),
                runtimeSummary.getStepId(),
                runtimeSummary.getStepAttemptId())) {
              runtimeSummary.markWaitSignal(tracingManager);
            } else {
              runtimeSummary.addTimeline(
                  TimelineLogEvent.info(
                      "Step is paused for workflowId: [%s], instanceId: [%d],"
                          + "runId: [%d], stepId: [%s], stepAttempt: [%d]",
                      workflowSummary.getWorkflowId(),
                      workflowSummary.getWorkflowInstanceId(),
                      workflowSummary.getWorkflowRunId(),
                      runtimeSummary.getStepId(),
                      runtimeSummary.getStepAttemptId()));
              doneWithExecute = true;
            }
            break;
          case WAITING_FOR_SIGNALS:
            if (signalsReady(workflowSummary, runtimeSummary)) {
              runtimeSummary.markEvaluateParam(tracingManager);
            } else {
              doneWithExecute = true;
            }
            break;
          case EVALUATING_PARAMS:
            doneWithExecute =
                evaluateParams(workflow, task, stepDefinition, workflowSummary, runtimeSummary);
            doneWithExecute = !isFirstPolling && doneWithExecute;
            break;
          case WAITING_FOR_PERMITS:
            if (permitsReady(workflowSummary, runtimeSummary)) {
              // If all required tag permits are acquired, then transition to starting.
              runtimeSummary.markStarting(tracingManager);
            } else {
              doneWithExecute = true;
            }
            break;
          case STARTING:
            doneWithExecute =
                stepRuntimeManager.start(workflowSummary, stepDefinition, runtimeSummary);
            break;
          case RUNNING:
            doneWithExecute =
                stepRuntimeManager.execute(workflowSummary, stepDefinition, runtimeSummary);
            break;
          case FINISHING:
            outputDataManager.validateAndMergeOutputParams(runtimeSummary);

            if (initializeAndSendOutputSignals(
                workflow, stepDefinition, workflowSummary, runtimeSummary)) {
              runtimeSummary.markTerminated(StepInstance.Status.SUCCEEDED, tracingManager);
            }
            break;
          case UNSATISFIED:
          case DISABLED:
          case SKIPPED:
          case SUCCEEDED:
          case COMPLETED_WITH_ERROR:
            evaluateNextConditionParams(workflow, stepDefinition, runtimeSummary);
            doneWithExecute = true;
            break;
          case FATALLY_FAILED: // Failure mode only applies to FATALLY_FAILED
            if (!runtimeSummary.isIgnoreFailureMode()) {
              if (FailureMode.IGNORE_FAILURE == stepDefinition.getFailureMode()) {
                runtimeSummary.markTerminated(
                    StepInstance.Status.COMPLETED_WITH_ERROR, tracingManager);
                runtimeSummary.addTimeline(
                    TimelineLogEvent.info(
                        "Step is failed but marked as COMPLETED_WITH_ERROR "
                            + "because its failure mode is IGNORE_FAILURE."));
                break;
              } else if (FailureMode.FAIL_IMMEDIATELY == stepDefinition.getFailureMode()) {
                terminateAllSteps(workflow, workflowSummary, stepDefinition.getId());
              }
            }
            // fall through, otherwise
          case INTERNALLY_FAILED: // Ignoring failure model as the error happens within Maestro
          case USER_FAILED:
          case PLATFORM_FAILED:
          case TIMEOUT_FAILED:
          case STOPPED:
          case TIMED_OUT:
            doneWithExecute = true;
            break;
          default:
            throw new MaestroInternalError(
                "Execution is at an unexpected state [%s] for step %s",
                runtimeSummary.getRuntimeState().getStatus(), runtimeSummary.getIdentity());
        }
      } catch (MaestroRetryableError error) {
        LOG.warn(
            "Got a MaestroRetryableError for the task [{}] in workflow [{}][{}], ",
            task.getTaskId(),
            workflow.getWorkflowName(),
            workflow.getWorkflowId(),
            error);
        throw error;
      } catch (Exception e) {
        LOG.warn(
            "Fatally failed to execute Maestro step for the task [{}] in workflow [{}][{}], get an exception:",
            task.getTaskId(),
            workflow.getWorkflowName(),
            workflow.getWorkflowId(),
            e);
        runtimeSummary.markInternalError(e, tracingManager);
        doneWithExecute = false; // the next while loop will handle it.
      }
    }
    return false;
  }

  /**
   * Stop all the steps in this workflow instance. If writing actions to DB failed, it throws a
   * retryable error.
   */
  private void terminateAllSteps(Workflow workflow, WorkflowSummary summary, String stepId) {
    WorkflowInstance toTerminate = new WorkflowInstance();
    toTerminate.setWorkflowId(summary.getWorkflowId());
    toTerminate.setWorkflowInstanceId(summary.getWorkflowInstanceId());
    toTerminate.setWorkflowRunId(summary.getWorkflowRunId());

    Map<String, Task> realTaskMap = TaskHelper.getUserDefinedRealTaskMap(workflow);
    // passing rollupBase as null because this overview is used to terminate steps
    // and thus having steps from prev runs is useless
    WorkflowRuntimeOverview overview =
        TaskHelper.computeOverview(objectMapper, summary, null, realTaskMap);
    toTerminate.setRuntimeDag(summary.getRuntimeDag());
    toTerminate.setRuntimeOverview(overview);
    try {
      actionDao.terminate(
          toTerminate,
          StepRuntime.SYSTEM_USER,
          Actions.WorkflowInstanceAction.STOP,
          String.format(
              "Stop all steps because step [%s] with FAIL_IMMEDIATELY mode is failed.", stepId));
    } catch (MaestroInternalError error) {
      throw error;
    } catch (RuntimeException e) { // Currently, assume all runtime exception is retryable
      throw new MaestroRetryableError(
          e, "Fail to add steps actions for termination and will retry");
    }
  }

  @VisibleForTesting
  void updateRetryDelayTimeToTimeline(StepRuntimeSummary runtimeSummary) {
    StepInstance.Status status = runtimeSummary.getRuntimeState().getStatus();
    if (status == StepInstance.Status.USER_FAILED
        || status == StepInstance.Status.PLATFORM_FAILED
        || status == StepInstance.Status.TIMEOUT_FAILED) {
      int nextRetryDelayInSecs =
          runtimeSummary
              .getStepRetry()
              .getNextRetryDelay(runtimeSummary.getRuntimeState().getStatus());
      String humanReadableRetryTime =
          DurationHelper.humanReadableFormat(Duration.ofSeconds(nextRetryDelayInSecs));
      runtimeSummary.addTimeline(
          TimelineLogEvent.info("Retrying task in [%s]", humanReadableRetryTime));
    }
  }

  private boolean signalsReady(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    return signalHandler.signalsReady(workflowSummary, runtimeSummary);
  }

  private void evaluateNextConditionParams(
      Workflow workflow, Step stepDefinition, StepRuntimeSummary runtimeSummary) {
    Map<String, Map<String, Object>> allStepOutputData = TaskHelper.getAllStepOutputData(workflow);
    WorkflowSummary workflowSummary =
        StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());

    boolean isSatisfied =
        runtimeSummary.getParams().get(Constants.STEP_SATISFIED_FIELD).asBoolean();

    // if satisfied but the param evaluation is not done yet
    boolean shouldGenerateParams =
        isSatisfied && runtimeSummary.getRuntimeState().getWaitPermitTime() == null;

    Map<String, Parameter> allStepParams =
        shouldGenerateParams
            ? stepRuntimeManager.getAllParams(stepDefinition, workflowSummary, runtimeSummary)
            : runtimeSummary.getParams();

    paramExtensionRepo.reset(
        allStepOutputData,
        Collections.emptyMap(),
        InstanceWrapper.from(workflowSummary, runtimeSummary));
    runtimeSummary
        .getTransition()
        .getSuccessors()
        .values()
        .forEach(
            param -> {
              if (isSatisfied) {
                paramEvaluator.parseStepParameter(
                    allStepOutputData,
                    workflowSummary.getParams(),
                    allStepParams,
                    param,
                    runtimeSummary.getStepId());
              } else {
                param.setEvaluatedResult(false);
                param.setEvaluatedTime(System.currentTimeMillis());
              }
            });
    paramExtensionRepo.clear();

    if (shouldGenerateParams) {
      runtimeSummary.mergeParams(
          allStepParams.entrySet().stream()
              .filter(e -> e.getValue().isEvaluated())
              .collect(MapHelper.toListMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
  }

  private void syncPendingUpdates(
      Task task,
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      boolean thrown) {
    StepInstance stepInstance = createStepInstance(task, workflowSummary, runtimeSummary);
    Optional<Details> result = stepSyncManager.sync(stepInstance, workflowSummary, runtimeSummary);
    if (result.isPresent()) {
      runtimeSummary.addTimeline(
          TimelineLogEvent.warn("Failed to sync due to error: " + result.get()));
      if (thrown) {
        throw new MaestroRetryableError(
            "Failed to sync data when the step %s is terminating due to error %s.",
            runtimeSummary.getIdentity(), result.get());
      }
    } else {
      runtimeSummary.cleanUp();
      // update task status only if sync succeeds.
      deriveTaskStatus(task, runtimeSummary);
    }
    task.getOutputData().put(Constants.STEP_RUNTIME_SUMMARY_FIELD, runtimeSummary);
  }

  private StepInstance createStepInstance(
      Task task, WorkflowSummary workflowSummary, StepRuntimeSummary stepSummary) {
    StepInstance stepInstance = new StepInstance();
    stepInstance.setWorkflowId(workflowSummary.getWorkflowId());
    stepInstance.setWorkflowInstanceId(workflowSummary.getWorkflowInstanceId());
    stepInstance.setWorkflowRunId(workflowSummary.getWorkflowRunId());
    stepInstance.setStepId(stepSummary.getStepId());
    stepInstance.setStepAttemptId(stepSummary.getStepAttemptId());
    stepInstance.setWorkflowUuid(workflowSummary.getWorkflowUuid());
    stepInstance.setStepUuid(stepSummary.getStepInstanceUuid());
    String correlationId =
        ObjectHelper.valueOrDefault(
            workflowSummary.getCorrelationId(), workflowSummary.getWorkflowUuid());
    stepInstance.setCorrelationId(correlationId); // use the workflow uuid by default
    stepInstance.setStepInstanceId(stepSummary.getStepInstanceId());

    if (stepSummary.getDbOperation() != DbOperation.UPDATE) {
      Step stepDefinition = StepHelper.retrieveStepDefinition(objectMapper, task.getInputData());
      stepInstance.setWorkflowVersionId(workflowSummary.getWorkflowVersionId());
      stepInstance.setOwner(workflowSummary.getRunProperties().getOwner());
      stepInstance.setDefinition(stepDefinition);
      stepInstance.setTags(stepSummary.getTags());
      stepInstance.setParams(stepSummary.getParams());
      stepInstance.setTransition(stepSummary.getTransition());
      stepInstance.setStepRetry(stepSummary.getStepRetry());
      stepInstance.setTimeoutInMillis(stepSummary.getTimeoutInMillis());
      stepInstance.setRuntimeState(stepSummary.getRuntimeState());
      stepInstance.setDependencies(stepSummary.getDependencies());
      stepInstance.setOutputs(stepSummary.getOutputs());
      stepInstance.setArtifacts(stepSummary.getArtifacts());
      stepInstance.setTimeline(stepSummary.getTimeline());
    }
    return stepInstance;
  }

  private boolean permitsReady(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    try {
      List<Tag> allTags = workflowSummary.deriveRuntimeTagPermits(runtimeSummary);
      allTags.addAll(runtimeSummary.getTags().getTags());

      TagPermitManager.Status tagPermitStatus =
          tagPermitAcquirer.acquire(allTags, runtimeSummary.getStepInstanceUuid());
      runtimeSummary.addTimeline(TimelineLogEvent.info(tagPermitStatus.getMessage()));
      return tagPermitStatus.isSuccess();
    } catch (RuntimeException e) { // Currently, assume all runtime exception is retryable
      runtimeSummary.addTimeline(
          TimelineLogEvent.warn(
              "Permit readiness check fails with an error [%s] and will retry", e.getMessage()));
      return false;
    }
  }

  /** From Maestro step instance status to conductor task status. */
  private void deriveTaskStatus(Task task, StepRuntimeSummary runtimeSummary) {
    switch (runtimeSummary.getRuntimeState().getStatus()) {
      case NOT_CREATED:
      case CREATED:
      case INITIALIZED:
      case PAUSED:
      case WAITING_FOR_SIGNALS:
      case EVALUATING_PARAMS:
      case WAITING_FOR_PERMITS:
      case STARTING:
      case RUNNING:
      case FINISHING:
        task.setStatus(Task.Status.IN_PROGRESS);
        break;
      case DISABLED:
      case UNSATISFIED:
      case SKIPPED:
      case SUCCEEDED:
      case COMPLETED_WITH_ERROR:
        task.setStatus(Task.Status.COMPLETED);
        break;
      case USER_FAILED:
      case PLATFORM_FAILED:
      case TIMEOUT_FAILED:
        task.setStatus(Task.Status.FAILED);
        task.setStartDelayInSeconds(
            runtimeSummary
                .getStepRetry()
                .getNextRetryDelay(runtimeSummary.getRuntimeState().getStatus()));
        break;
      case FATALLY_FAILED:
      case INTERNALLY_FAILED:
        task.setStatus(Task.Status.FAILED);
        break;
      case STOPPED:
        task.setStatus(Task.Status.CANCELED);
        break;
      case TIMED_OUT:
        task.setStatus(Task.Status.TIMED_OUT);
        break;
      default:
        throw new MaestroInternalError(
            "Entered an unexpected state [%s] for step %s",
            runtimeSummary.getRuntimeState().getStatus(), runtimeSummary.getIdentity());
    }
  }

  /**
   * Cancel conductor task execution. Throw exceptions if failed and then will be retried.
   *
   * @param workflow Workflow for which the task is being started
   * @param task Instance of the Task
   * @param executor Workflow Executor
   */
  @Override
  public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
    WorkflowSummary workflowSummary =
        StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());

    StepRuntimeSummary runtimeSummary;
    try {
      runtimeSummary = StepHelper.retrieveRuntimeSummary(objectMapper, task.getOutputData());
    } catch (RuntimeException e) {
      // treat the maestro step does not exist when its step runtime summary is not created
      return;
    }

    if (workflow.getStatus() == Workflow.WorkflowStatus.TIMED_OUT) {
      terminate(workflowSummary, runtimeSummary, StepInstance.Status.TIMED_OUT);
    } else {
      terminate(workflowSummary, runtimeSummary, StepInstance.Status.STOPPED);
    }

    if (!runtimeSummary.isSynced()) {
      syncPendingUpdates(task, workflowSummary, runtimeSummary, true);
    }
  }

  /** Terminate the execution without sync. */
  private void terminate(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      StepInstance.Status toStatus) {
    try {
      StepInstance.Status status = runtimeSummary.getRuntimeState().getStatus();
      if (status != StepInstance.Status.NOT_CREATED && !status.isTerminal()) {
        if (toStatus == StepInstance.Status.TIMED_OUT && !runtimeSummary.getType().isLeaf()) {
          // only time out itself and other tasks will time out themselves
          actionDao.terminate(
              workflowSummary,
              runtimeSummary.getStepId(),
              StepRuntime.SYSTEM_USER,
              Actions.StepInstanceAction.STOP,
              "step is stopped due to timeout");
        }
        stepRuntimeManager.terminate(workflowSummary, runtimeSummary, toStatus);
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to terminate step {}{} due to error ",
          workflowSummary.getIdentity(),
          runtimeSummary.getIdentity(),
          e);
      throw e;
    }
  }

  @Override
  public boolean isAsync() {
    return true;
  }
}
