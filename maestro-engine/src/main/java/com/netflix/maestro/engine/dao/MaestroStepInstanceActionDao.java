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
package com.netflix.maestro.engine.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.database.AbstractDatabaseDao;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.engine.utils.TimeUtils;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroTimeoutException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.FailureMode;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * Dao for actions to the step instance in a non-terminal workflow instance.
 *
 * <p>the final step instance event callback will do the step action cleanup to make sure no stale
 * data.
 */
@Slf4j
public class MaestroStepInstanceActionDao extends AbstractDatabaseDao {
  private static final long ACTION_TIMEOUT = 30 * 1000L; // 30 sec
  private static final long CHECK_INTERVAL = 1000L; // 1 sec

  private static final String INSERT_ACTION_QUERY =
      "INSERT INTO maestro_step_instance_action (payload) VALUES (?) ON CONFLICT DO NOTHING";
  private static final String INSTANCE_CONDITION =
      "workflow_id=? AND workflow_instance_id=? AND workflow_run_id=?";
  private static final String CONDITION_POSTFIX = "(" + INSTANCE_CONDITION + " AND step_id=?)";
  private static final String DELETE_ACTION_PREFIX =
      "DELETE FROM maestro_step_instance_action WHERE ";
  private static final String DELETE_ACTION_QUERY = DELETE_ACTION_PREFIX + CONDITION_POSTFIX;
  private static final String GET_ACTION_QUERY =
      "SELECT payload, create_ts FROM maestro_step_instance_action WHERE " + CONDITION_POSTFIX;
  private static final String UPSERT_ACTIONS_QUERY_TEMPLATE =
      "UPSERT INTO maestro_step_instance_action (payload,create_ts) VALUES %s";
  private static final String VALUE_PLACE_HOLDER = "(?,CURRENT_TIMESTAMP)";
  private static final String DELETE_ACTIONS_QUERY = DELETE_ACTION_PREFIX + INSTANCE_CONDITION;

  private final MaestroStepInstanceDao stepInstanceDao;
  private final MaestroJobEventPublisher eventPublisher;

  /** step instance action dao constructor. */
  public MaestroStepInstanceActionDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroStepInstanceDao stepInstanceDao,
      MaestroJobEventPublisher eventPublisher,
      MaestroMetrics metrics) {
    super(dataSource, objectMapper, config, metrics);
    this.stepInstanceDao = stepInstanceDao;
    this.eventPublisher = eventPublisher;
  }

  /** restart a restartable step instance in a non-terminal workflow instance. */
  public RunResponse restartDirectly(
      RunResponse restartStepInfo, RunRequest runRequest, boolean blocking) {
    WorkflowInstance instance = restartStepInfo.getInstance();
    String stepId = restartStepInfo.getStepId();
    validateStepId(instance, stepId, Actions.StepInstanceAction.RESTART);
    StepInstance stepInstance =
        getStepInstanceAndValidate(instance, stepId, runRequest.getRestartConfig());
    // prepare payload and then add to db
    StepAction stepAction = StepAction.createRestart(stepInstance, runRequest);
    saveAction(stepInstance, stepAction);
    if (blocking) {
      return waitResponseWithTimeout(stepInstance, stepAction);
    } else {
      return RunResponse.from(stepInstance, stepAction.toTimelineEvent());
    }
  }

  /** bypass the signal dependencies. */
  public StepInstanceActionResponse bypassStepDependencies(
      WorkflowInstance instance, String stepId, User user, boolean blocking) {
    validateStepId(instance, stepId, Actions.StepInstanceAction.BYPASS_STEP_DEPENDENCIES);
    StepInstance stepInstance =
        getStepInstanceAndValidateBypassStepDependencyConditions(instance, stepId);

    StepAction stepAction = StepAction.createBypassStepDependencies(stepInstance, user);
    saveAction(stepInstance, stepAction);
    if (blocking) {
      return waitBypassStepDependenciesResponseWithTimeout(stepInstance, stepAction);
    } else {
      return createActionResponseFrom(stepInstance, null, stepAction.toTimelineEvent());
    }
  }

  private void validateStepId(
      WorkflowInstance instance, String stepId, Actions.StepInstanceAction action) {
    if (instance.getStatus().isTerminal()) {
      throw new MaestroInvalidStatusException(
          "Cannot manually %s the step [%s] as its workflow instance status [%s] is terminal",
          action.name(), stepId, instance.getStatus());
    }
    if (!instance.getRuntimeDag().containsKey(stepId)) {
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "Cannot manually %s the step [%s] because the latest workflow run %s "
              + "with status [%s] does not contain it in the runtime dag.",
          action.name(),
          stepId,
          instance.getIdentity(),
          instance.getStatus());
    }
  }

  /**
   * Before get step instance, it does multiple validation checks, including aggregated view check,
   * status check, and is restartable and retryable check, and also checks the failure mode as well.
   */
  private StepInstance getStepInstanceAndValidate(
      WorkflowInstance instance, String stepId, RestartConfig restartConfig) {
    StepAggregatedView aggregatedView =
        instance.getAggregatedInfo().getStepAggregatedViews().get(stepId);
    if (aggregatedView == null || aggregatedView.getWorkflowRunId() == null) {
      throw new MaestroInvalidStatusException(
          "Cannot manually RESTART the step [%s][%s] as it does not exist (either not started or reset previously)",
          instance.getIdentity(), stepId);
    }
    StepInstance stepInstance =
        stepInstanceDao.getStepInstance(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            aggregatedView.getWorkflowRunId(),
            stepId,
            Constants.LATEST_INSTANCE_RUN);

    if (isForeachStepRunningAndRestartable(stepInstance)) {
      long iterationId = getRestartForeachIterationFromConfig(restartConfig, stepInstance);
      if (iterationId > 0) { // iteration is restartable
        LOG.info(
            "restarting foreach iteration [{}] for foreach step {}",
            iterationId,
            stepInstance.getIdentity());
        return stepInstance;
      } else if (iterationId < 0) { // iteration is non-restartable
        throw new MaestroResourceConflictException(
            "The foreach iteration [%s] is not ready to be restarted for foreach step %s",
            -iterationId, stepInstance.getIdentity());
      }
    }

    if (!stepInstance.getRuntimeState().getStatus().isTerminal()
        || stepInstance.getRuntimeState().getStatus().isComplete()) {
      throw new MaestroInvalidStatusException(
          "Cannot manually RESTART the step %s as its status [%s] is either non-terminal or complete "
              + "while the workflow instance or its ancestor workflow instances is still non-terminal.",
          stepInstance.getIdentity(), stepInstance.getRuntimeState().getStatus());
    }
    // both checks are only for a step instance within the same workflow instance.
    if (instance.getWorkflowRunId() == aggregatedView.getWorkflowRunId()) {
      if (stepInstance.getStepRetry().isRetryable()) {
        throw new MaestroInvalidStatusException(
            "Cannot manually RESTART the step %s as the system is still retrying. The current step retry is [%s]",
            stepInstance.getIdentity(), stepInstance.getStepRetry());
      }
      if (stepInstance.getDefinition().getFailureMode() != FailureMode.FAIL_AFTER_RUNNING) {
        throw new MaestroInvalidStatusException(
            "Cannot manually RESTART the step %s as step failure mode [%s] is not FAIL_AFTER_RUNNING",
            stepInstance.getIdentity(), stepInstance.getDefinition().getFailureMode());
      }
    }
    // mutate step instance runId to match the workflow instance of the current run
    stepInstance.setWorkflowRunId(instance.getWorkflowRunId());
    return stepInstance;
  }

  private boolean isForeachStepRunningAndRestartable(StepInstance stepInstance) {
    return stepInstance.getDefinition().getType() == StepType.FOREACH
        && stepInstance.getRuntimeState().getStatus() == StepInstance.Status.RUNNING
        && stepInstance.getDefinition().getFailureMode() != FailureMode.FAIL_IMMEDIATELY
        && stepInstance.getArtifacts() != null
        && stepInstance.getArtifacts().get(Artifact.Type.FOREACH.key()) != null;
  }

  /**
   * Get foreach iteration id for a restart config. Note that currently the restart is only for an
   * iteration, not for the foreach step itself. This only applies to the foreach iteration restart
   * while foreach step is running.
   *
   * @param restartConfig restart configuration
   * @param stepInstance foreach step instance
   * @return return a positive iteration id if the iteration is restartable, return negative if the
   *     iteration is not restartable, return 0 if unknown.
   */
  private long getRestartForeachIterationFromConfig(
      RestartConfig restartConfig, StepInstance stepInstance) {
    Artifact artifact = stepInstance.getArtifacts().get(Artifact.Type.FOREACH.key());
    if (artifact != null) {
      ForeachArtifact foreachArtifact = artifact.asForeach();
      List<RestartConfig.RestartNode> path = restartConfig.getRestartPath();
      if (path != null && path.size() > 1) {
        RestartConfig.RestartNode restartNode = path.get(path.size() - 2);
        if (Objects.equals(restartNode.getWorkflowId(), foreachArtifact.getForeachWorkflowId())) {
          long restartForeachIterationId = restartNode.getInstanceId();
          // index is 0-based and iteration id is 1-based.
          if (foreachArtifact.getNextLoopIndex() >= restartForeachIterationId) {
            if (foreachArtifact.getPendingAction() == null
                && foreachArtifact
                    .getForeachOverview()
                    .isForeachIterationRestartable(restartForeachIterationId)) {
              return restartForeachIterationId;
            } else {
              return -restartForeachIterationId;
            }
          }
        }
      }
    }
    return 0;
  }

  private StepInstance getStepInstanceAndValidateBypassStepDependencyConditions(
      WorkflowInstance instance, String stepId) {
    StepInstance stepInstance =
        stepInstanceDao.getStepInstance(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId(),
            stepId,
            Constants.LATEST_INSTANCE_RUN);

    if (stepInstance.getRuntimeState().getStatus() != StepInstance.Status.WAITING_FOR_SIGNALS) {
      throw new MaestroInvalidStatusException(
          "Cannot manually bypass-step-dependencies the step as its status [%s] is not waiting for signals",
          stepInstance.getRuntimeState().getStatus());
    }
    return stepInstance;
  }

  private void saveAction(StepInstance stepInstance, StepAction stepAction) {
    String payload = toJson(stepAction);
    int ret =
        withMetricLogError(
            () -> withRetryableUpdate(INSERT_ACTION_QUERY, stmt -> stmt.setString(1, payload)),
            "saveAction",
            "Failed to save the action for step {}",
            stepInstance.getIdentity());
    if (ret != SUCCESS_WRITE_SIZE) {
      throw new MaestroResourceConflictException(
          "There is an ongoing action for this step %s and please try it again later.",
          stepInstance.getIdentity());
    }
    publishUserActionEvent(StepInstanceWakeUpEvent.create(stepInstance, stepAction));
  }

  private RunResponse waitResponseWithTimeout(StepInstance stepInstance, StepAction action) {
    try {
      long startTime = System.currentTimeMillis();
      boolean isForeachStepRunning = isForeachStepRunningAndRestartable(stepInstance);
      while (System.currentTimeMillis() - startTime < ACTION_TIMEOUT) {
        StepInstance stepView =
            stepInstanceDao.getStepInstanceView(
                stepInstance.getWorkflowId(),
                stepInstance.getWorkflowInstanceId(),
                stepInstance.getStepId());
        if (isForeachStepRunning) {
          long iterationId =
              getRestartForeachIterationFromConfig(action.getRestartConfig(), stepView);
          LOG.debug(
              "Foreach step {} is restarting foreach iteration [{}]",
              stepInstance.getIdentity(),
              iterationId);
          if (iterationId < 0) { // the iteration becomes non-restartable, meaning it is restarted.
            return RunResponse.from(stepInstance, action.toTimelineEvent());
          }
        } else {
          StepRuntimeState runtimeState = stepView.getRuntimeState();
          LOG.debug("step instance {}'s state is [{}]", stepInstance.getIdentity(), runtimeState);
          if (runtimeState != null
              && (!runtimeState.getStatus().isTerminal()
                  // actions might end the instance already, so also checking create time.
                  || ObjectHelper.valueOrDefault(runtimeState.getCreateTime(), 0L) > startTime)) {
            return RunResponse.from(stepInstance, action.toTimelineEvent());
          }
        }

        TimeUtils.sleep(CHECK_INTERVAL);
      }
    } finally {
      deleteAction(stepInstance, action.getAction());
    }

    throw new MaestroTimeoutException(
        "RESTART action for the step %s is timed out and please retry it.",
        stepInstance.getIdentity());
  }

  private StepInstanceActionResponse waitBypassStepDependenciesResponseWithTimeout(
      StepInstance stepInstance, StepAction action) {
    try {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < ACTION_TIMEOUT) {

        StepRuntimeState state =
            stepInstanceDao.getStepInstanceRuntimeState(
                stepInstance.getWorkflowId(),
                stepInstance.getWorkflowInstanceId(),
                stepInstance.getWorkflowRunId(),
                stepInstance.getStepId(),
                Long.toString(stepInstance.getStepAttemptId()));
        if (state.getStatus() == StepInstance.Status.WAITING_FOR_SIGNALS) {
          TimeUtils.sleep(CHECK_INTERVAL);
        } else {
          return createActionResponseFrom(stepInstance, state, action.toTimelineEvent());
        }
      }
    } finally {
      deleteAction(stepInstance, action.getAction());
    }

    throw new MaestroTimeoutException(
        "bypass-step-dependencies action for the step %s is timed out and please retry it.",
        stepInstance.getIdentity());
  }

  @VisibleForTesting
  int deleteAction(StepInstance stepInstance, Actions.StepInstanceAction action) {
    // as the action is an enum, this should be safe without sql injection risk.
    String sql =
        action == null
            ? DELETE_ACTION_QUERY
            : DELETE_ACTION_QUERY + " AND payload->>'action'='" + action.name() + "'";
    return withMetricLogError(
        () ->
            withRetryableUpdate(
                sql,
                stmt -> {
                  int idx = 0;
                  stmt.setString(++idx, stepInstance.getWorkflowId());
                  stmt.setLong(++idx, stepInstance.getWorkflowInstanceId());
                  stmt.setLong(++idx, stepInstance.getWorkflowRunId());
                  stmt.setString(++idx, stepInstance.getStepId());
                }),
        "deleteAction",
        "Failed to delete the action for step {}",
        stepInstance.getIdentity());
  }

  /**
   * Terminate the step instance. Even it is timed out, the termination will continue because we
   * should not leave the termination in partial stopped states. The final step instance event
   * callback will do the cleanup.
   */
  public StepInstanceActionResponse terminate(
      WorkflowInstance instance,
      String stepId,
      User user,
      Actions.StepInstanceAction action,
      boolean blocking) {
    validateStepId(instance, stepId, action);

    StepInstance stepInstance =
        stepInstanceDao.getStepInstance(
            instance.getWorkflowId(),
            instance.getWorkflowInstanceId(),
            instance.getWorkflowRunId(),
            stepId,
            Constants.LATEST_INSTANCE_RUN);

    if (!stepInstance.getRuntimeState().getStatus().shouldWakeup()) {
      throw new MaestroInvalidStatusException(
          "Cannot manually %s the step %s as it is in a terminal state [%s]",
          action.name(), stepInstance.getIdentity(), stepInstance.getRuntimeState().getStatus());
    }

    // prepare payload and then add it to db
    StepAction stepAction =
        StepAction.createTerminate(
            action, stepInstance, user, "manual step instance API call", false);
    saveAction(stepInstance, stepAction);

    if (blocking) {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < ACTION_TIMEOUT) {
        StepRuntimeState state =
            stepInstanceDao.getStepInstanceRuntimeState(
                stepInstance.getWorkflowId(),
                stepInstance.getWorkflowInstanceId(),
                stepInstance.getWorkflowRunId(),
                stepInstance.getStepId(),
                Constants.LATEST_INSTANCE_RUN);
        if (!state.getStatus().shouldWakeup()) {
          return createActionResponseFrom(stepInstance, state, stepAction.toTimelineEvent());
        }
        TimeUtils.sleep(CHECK_INTERVAL);
      }

      throw new MaestroTimeoutException(
          "%s action for the step %s is timed out. No retry is needed and maestro will eventually complete the action.",
          action.name(), stepInstance.getIdentity());
    } else {
      return createActionResponseFrom(stepInstance, null, stepAction.toTimelineEvent());
    }
  }

  private StepInstanceActionResponse createActionResponseFrom(
      StepInstance instance, StepRuntimeState state, TimelineEvent event) {
    return StepInstanceActionResponse.builder()
        .workflowId(instance.getWorkflowId())
        .workflowInstanceId(instance.getWorkflowInstanceId())
        .workflowRunId(instance.getWorkflowRunId())
        .stepId(instance.getStepId())
        .stepAttemptId(instance.getStepAttemptId())
        .stepRuntimeState(state)
        .timelineEvent(event)
        .build();
  }

  /**
   * Get the step action. If failed to get the action, treat it as no action in DB. The caller is
   * expected to check the action again.
   */
  public Optional<StepAction> tryGetAction(WorkflowSummary summary, String stepId) {
    try {
      return getAction(summary, stepId);
    } catch (RuntimeException ae) {
      // treat it as no action and will check the action again in the next cycle
      LOG.warn(
          "tryUpdateByAction will check the action again in the next turn due to exception ", ae);
      return Optional.empty();
    }
  }

  private Optional<StepAction> getAction(WorkflowSummary summary, String stepId) {
    Initiator initiator = summary.getInitiator();
    List<UpstreamInitiator.Info> path = new ArrayList<>();
    StringBuilder sqlBuilder = new StringBuilder(GET_ACTION_QUERY);
    if (initiator instanceof UpstreamInitiator) {
      path.addAll(((UpstreamInitiator) initiator).getAncestors());
      sqlBuilder
          .append(" OR (")
          .append(String.join(") OR (", Collections.nCopies(path.size(), CONDITION_POSTFIX)))
          .append(')');
    }
    String sql = sqlBuilder.toString();

    UpstreamInitiator.Info self = new UpstreamInitiator.Info();
    self.setWorkflowId(summary.getWorkflowId());
    self.setInstanceId(summary.getWorkflowInstanceId());
    self.setRunId(summary.getWorkflowRunId());
    self.setStepId(stepId);
    path.add(self);

    List<StepAction> allActions =
        withMetricLogError(
            () ->
                withRetryableQuery(
                    sql,
                    stmt -> {
                      int idx = 0;
                      for (UpstreamInitiator.Info node : path) {
                        stmt.setString(++idx, node.getWorkflowId());
                        stmt.setLong(++idx, node.getInstanceId());
                        stmt.setLong(++idx, node.getRunId());
                        stmt.setString(++idx, node.getStepId());
                      }
                    },
                    result -> {
                      List<StepAction> actions = new ArrayList<>();
                      while (result.next()) {
                        StepAction res =
                            fromJson(result.getString(PAYLOAD_COLUMN), StepAction.class);
                        res.setCreateTime(result.getTimestamp("create_ts").getTime());
                        actions.add(res);
                      }
                      return actions;
                    }),
            "getActions",
            "Failed to get the actions for step {}[{}] with parents",
            summary.getIdentity(),
            stepId);

    AtomicInteger ordinal = new AtomicInteger(0);
    Map<UpstreamInitiator.Info, Integer> orders =
        path.stream()
            .collect(Collectors.toMap(Function.identity(), p -> ordinal.incrementAndGet()));

    allActions.sort(
        (a1, a2) ->
            Integer.compare(
                orders.getOrDefault(a2.toInfo(), 0), orders.getOrDefault(a1.toInfo(), 0)));

    // pick the final action from all of them.
    StepAction stepAction = null;
    for (StepAction action : allActions) {
      if (action.getAction() != null && action.getAction().isUsingUpstream()) {
        stepAction = action;
        break;
      } else if (System.currentTimeMillis() - action.getCreateTime() < ACTION_TIMEOUT
          && action.getWorkflowId().equals(self.getWorkflowId())
          && action.getWorkflowInstanceId() == self.getInstanceId()
          && action.getWorkflowRunId() == self.getRunId()
          && action.getStepId().equals(self.getStepId())) {
        stepAction = action;
      }
    }
    if (stepAction != null) {
      LOG.info(
          "Pick a pending action [{}] for step {}[{}] among total [{}] pending actions: [{}]",
          stepAction,
          summary.getIdentity(),
          stepId,
          allActions.size(),
          allActions);
    }
    return Optional.ofNullable(stepAction);
  }

  /** Delete remaining actions of any steps for a given workflow instance run. */
  public int cleanUp(String workflowId, long instanceId, long runId) {
    int deleted =
        withMetricLogError(
            () ->
                withRetryableUpdate(
                    DELETE_ACTIONS_QUERY,
                    stmt -> {
                      int idx = 0;
                      stmt.setString(++idx, workflowId);
                      stmt.setLong(++idx, instanceId);
                      stmt.setLong(++idx, runId);
                    }),
            "cleanUp",
            "Failed to delete actions for any steps in workflow [{}][{}][{}]",
            workflowId,
            instanceId,
            runId);
    LOG.debug(
        "cleanup [{}] remaining actions for workflow instance: [{}][{}][{}]",
        deleted,
        workflowId,
        instanceId,
        runId);
    return deleted;
  }

  /** Delete any remaining action for a given step instance. */
  public int cleanUp(StepInstanceUpdateJobEvent jobEvent) {
    StepInstance instance = new StepInstance();
    instance.setWorkflowId(jobEvent.getWorkflowId());
    instance.setWorkflowInstanceId(jobEvent.getWorkflowInstanceId());
    instance.setWorkflowRunId(jobEvent.getWorkflowRunId());
    instance.setStepId(jobEvent.getStepId());
    int deleted = deleteAction(instance, null);
    LOG.debug("cleanup [{}] remaining actions based on job event: {}", deleted, jobEvent);
    return deleted;
  }

  /**
   * Terminate a running step instance in a given workflow instance. It overwrites the current
   * action.
   */
  public void terminate(
      WorkflowSummary summary,
      String stepId,
      User user,
      Actions.StepInstanceAction action,
      String reason) {
    LOG.info(
        "User [{}] {}-ing step instance: {}[{}]",
        user.getName(),
        action.name(),
        summary.getIdentity(),
        stepId);
    StepInstance stepInstance = new StepInstance();
    stepInstance.setWorkflowId(summary.getWorkflowId());
    stepInstance.setWorkflowInstanceId(summary.getWorkflowInstanceId());
    stepInstance.setWorkflowRunId(summary.getWorkflowRunId());
    stepInstance.setStepId(stepId);
    stepInstance.setMaxGroupNum(summary.getMaxGroupNum());
    StepAction stepAction = StepAction.createTerminate(action, stepInstance, user, reason, false);

    upsertActions(
        summary.getIdentity() + "[" + stepId + "]", Collections.singletonList(toJson(stepAction)));
    publishUserActionEvent(StepInstanceWakeUpEvent.create(stepInstance, stepAction));
  }

  /**
   * Terminate all non-terminal steps in a given workflow instance. It overwrites any current action
   * for all steps.
   */
  public int terminate(
      WorkflowInstance instance, User user, Actions.WorkflowInstanceAction action, String reason) {
    LOG.info(
        "User [{}] {}-ing workflow instance: {}",
        user.getName(),
        action.name(),
        instance.getIdentity());

    Actions.StepInstanceAction stepInstanceAction;
    if (action == Actions.WorkflowInstanceAction.STOP) {
      stepInstanceAction = Actions.StepInstanceAction.STOP;
    } else if (action == Actions.WorkflowInstanceAction.KILL) {
      stepInstanceAction = Actions.StepInstanceAction.KILL;
    } else {
      throw new MaestroInternalError("Unsupported workflow terminate action: " + action);
    }

    Map<String, StepRuntimeState> stepStates =
        instance.getRuntimeOverview().decodeStepOverview(instance.getRuntimeDag());

    StepInstance stepInstance = new StepInstance();
    stepInstance.setWorkflowId(instance.getWorkflowId());
    stepInstance.setWorkflowInstanceId(instance.getWorkflowInstanceId());
    stepInstance.setWorkflowRunId(instance.getWorkflowRunId());

    // prepare all action strings.
    List<String> payloads =
        instance.getRuntimeDag().keySet().stream()
            .filter(stepId -> incomplete(stepStates, stepId))
            .map(
                stepId -> {
                  stepInstance.setStepId(stepId);
                  StepAction stepAction =
                      StepAction.createTerminate(
                          stepInstanceAction, stepInstance, user, reason, true);
                  return toJson(stepAction);
                })
            .toList();

    // batch upsert them into DB.
    String workflowIdentity = instance.getIdentity();
    int upsert =
        IntStream.range(0, payloads.size())
            .boxed()
            .collect(
                Collectors.groupingBy(
                    partition -> (partition / Constants.TERMINATE_BATCH_LIMIT),
                    Collectors.mapping(payloads::get, Collectors.toList())))
            .values()
            .stream()
            // There is a chance of inconsistency if batches fail in the middle for manual
            // termination case. Expect users to manually retry to terminate all of them.
            .mapToInt(payloadList -> upsertActions(workflowIdentity, payloadList))
            .sum();

    LOG.debug(
        "Found [{}] incomplete steps and upsert [{}] step actions for workflow {} to the step action table.",
        payloads.size(),
        upsert,
        workflowIdentity);

    // publish the action job event.
    publishUserActionEvent(StepInstanceWakeUpEvent.create(instance, action));
    return upsert;
  }

  /** Batch upsert step actions, payloads must fit into a single batch. */
  private int upsertActions(String identity, List<String> payloads) {
    String sql =
        String.format(
            UPSERT_ACTIONS_QUERY_TEMPLATE,
            String.join(",", Collections.nCopies(payloads.size(), VALUE_PLACE_HOLDER)));
    return withMetricLogError(
        () ->
            withRetryableUpdate(
                sql,
                stmt -> {
                  int idx = 0;
                  for (String payload : payloads) {
                    stmt.setString(++idx, payload);
                  }
                }),
        "upsertActions",
        "Failed to upsert the step actions for [{}]",
        identity);
  }

  /** Will consider all possible running steps, including failed ones. */
  private boolean incomplete(Map<String, StepRuntimeState> stepStates, String stepId) {
    return !stepStates.containsKey(stepId) || !stepStates.get(stepId).getStatus().isComplete();
  }

  /**
   * Try to send wake up event for a user action. It's best effort and won't throw an exception if
   * failed. The failure rate can be monitored using event publisher failure metric using jobEvent
   * class as tag.
   */
  private void publishUserActionEvent(StepInstanceWakeUpEvent event) {
    Optional<Details> details = eventPublisher.publish(event);
    details.ifPresent(
        detail ->
            LOG.warn(
                "Action event publish failed: {}. With error: {}",
                detail.getMessage(),
                detail.getErrors()));
  }
}
