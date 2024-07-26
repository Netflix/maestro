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
package com.netflix.maestro.engine.execution;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.db.DbOperation;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.tracing.MaestroTracingContext;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.SignalReference;
import com.netflix.maestro.models.instance.SignalStepOutputs;
import com.netflix.maestro.models.instance.StepDependencies;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstanceTransition;
import com.netflix.maestro.models.instance.StepOutputs;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.validations.TagListConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Step instance runtime summary, it includes the generated data at runtime. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "step_id",
      "step_attempt_id",
      "step_instance_uuid",
      "step_name",
      "step_instance_id",
      "tags",
      "type",
      "sub_type",
      "params",
      "transition",
      "step_retry",
      "timeout_in_millis",
      "synced",
      "db_ops",
      "runtime_state",
      "dependencies",
      "outputs",
      "artifacts",
      "timeline",
      "pending_records",
      "tracing_context",
      "step_run_params",
      "restart_config"
    },
    alphabetic = true)
@JsonDeserialize(builder = StepRuntimeSummary.StepRuntimeSummaryBuilder.class)
@Getter
@EqualsAndHashCode
@ToString
public final class StepRuntimeSummary {
  private final String stepId;
  private final long stepAttemptId;
  private final String stepInstanceUuid;
  private final String stepName;
  private final long stepInstanceId;
  @Valid @TagListConstraint private final TagList tags;
  private final StepType type;
  private final String subType;
  @Valid private final Map<String, Parameter> params;
  @Valid private final StepInstanceTransition transition;
  @NotNull @Valid private final StepInstance.StepRetry stepRetry;
  @Nullable @Setter private Long timeoutInMillis;

  private boolean synced;
  private DbOperation dbOperation;
  @Valid @NotNull private final StepRuntimeState runtimeState;

  @Valid private Map<StepDependencyType, StepDependencies> dependencies;
  @Valid private Map<StepOutputsDefinition.StepOutputType, StepOutputs> outputs;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private final Map<String, Artifact> artifacts;

  @Valid private final Timeline timeline;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private final List<StepInstanceUpdateJobEvent.StepInstancePendingRecord> pendingRecords;

  @Nullable private final MaestroTracingContext tracingContext;

  @Setter @Nullable private Map<String, ParamDefinition> stepRunParams;
  @Setter @Nullable private RestartConfig restartConfig;

  // flag to ignore failure mode or not, default is false, won't persist it.
  @JsonIgnore @Nullable private boolean ignoreFailureMode;
  // to pass the latest pending action to step runtime used by foreach restart
  @JsonIgnore @Setter @Nullable private StepAction pendingAction;

  @Builder
  StepRuntimeSummary(
      @NotNull String stepId,
      long stepAttemptId,
      @NotNull String stepInstanceUuid,
      String stepName,
      long stepInstanceId,
      @Valid @TagListConstraint TagList tags,
      @NotNull StepType type,
      String subType,
      @Valid @NotNull Map<String, Parameter> params,
      @Valid @NotNull StepInstanceTransition transition,
      @Valid @NotNull StepInstance.StepRetry stepRetry,
      Long timeoutInMillis,
      boolean synced,
      @NotNull DbOperation dbOperation,
      StepRuntimeState runtimeState,
      Map<StepDependencyType, StepDependencies> dependencies,
      Map<StepOutputsDefinition.StepOutputType, StepOutputs> outputs,
      Map<String, Artifact> artifacts,
      Timeline timeline,
      List<StepInstanceUpdateJobEvent.StepInstancePendingRecord> pendingRecords,
      MaestroTracingContext tracingContext,
      @Nullable Map<String, ParamDefinition> stepRunParams,
      @Nullable RestartConfig restartConfig) {
    this.stepId = stepId;
    this.stepAttemptId = stepAttemptId;
    this.stepInstanceUuid = stepInstanceUuid;
    this.stepName = stepName;
    this.stepInstanceId = stepInstanceId;
    this.tags = tags == null ? new TagList(null) : tags;
    this.type = type;
    this.subType = subType;
    this.params = Parameter.preprocessInstanceParams(params);
    this.transition = transition;
    this.stepRetry = stepRetry;
    this.timeoutInMillis = timeoutInMillis;
    this.dbOperation = dbOperation; // never be null
    this.synced = synced;
    this.runtimeState = runtimeState == null ? new StepRuntimeState() : runtimeState;
    this.dependencies = dependencies;
    this.outputs = outputs;
    this.artifacts = artifacts == null ? new LinkedHashMap<>() : artifacts;
    this.timeline = timeline == null ? new Timeline(null) : timeline;
    this.pendingRecords = pendingRecords == null ? new ArrayList<>() : pendingRecords;
    this.tracingContext = tracingContext;
    this.stepRunParams = stepRunParams;
    this.restartConfig = restartConfig;
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class StepRuntimeSummaryBuilder {}

  /** merge evaluated params. */
  public void mergeParams(Map<String, Parameter> input) {
    params.putAll(Parameter.preprocessInstanceParams(input));
    runtimeState.setModifyTime(System.currentTimeMillis());
    synced = false;
  }

  /** merge tags. */
  public void mergeTags(List<Tag> input) {
    tags.merge(input);
    runtimeState.setModifyTime(System.currentTimeMillis());
    synced = false;
  }

  /** Initializes the dependencies for the given params. */
  public void initializeStepDependenciesSummaries(
      Map<StepDependencyType, List<MapParameter>> dependenciesParameters) {
    this.dependencies =
        dependenciesParameters.entrySet().stream()
            .collect(
                MapHelper.toListMap(
                    Map.Entry::getKey, e -> new StepDependencies(e.getKey(), e.getValue())));
    synced = false;
  }

  /**
   * Initialize outputs.
   *
   * @param outputParameters output signals
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "OperatorWrap"})
  public void initializeOutputs(
      Map<StepOutputsDefinition.StepOutputType, List<MapParameter>> outputParameters) {
    if (outputs == null) {
      outputs = new LinkedHashMap<>();
    }
    for (Map.Entry<StepOutputsDefinition.StepOutputType, List<MapParameter>> entry :
        outputParameters.entrySet()) {
      if (entry.getKey().equals(StepOutputsDefinition.StepOutputType.SIGNAL)) {
        List<MapParameter> outputParams = entry.getValue();
        outputs.put(
            StepOutputsDefinition.StepOutputType.SIGNAL,
            new SignalStepOutputs(
                outputParams.stream()
                    .map(p -> new SignalStepOutputs.SignalStepOutput(p, null))
                    .collect(Collectors.toList())));
      }
    }
    synced = false;
  }

  /** Updates the signal status based on the response from Signal service. */
  public void updateSignalStatus(
      MapParameter signalParams,
      StepDependencyMatchStatus signalStatus,
      SignalReference reference) {
    StepDependencies signalDependencies = getSignalDependencies();
    if (signalDependencies != null) {
      signalDependencies.getStatuses().stream()
          .filter(s -> s.getParams().equals(signalParams))
          .forEach(s -> s.updateStatus(reference, signalStatus));
      synced = false;
    }
  }

  /** By passes the step dependencies. */
  public void byPassStepDependencies(User user, long createTime) {
    if (dependencies != null) {
      dependencies.forEach((k, v) -> v.bypass(user, createTime));
      synced = false;
    }
  }

  /** Returns signal dependencies if present. */
  @JsonIgnore
  public StepDependencies getSignalDependencies() {
    return dependencies != null ? dependencies.get(StepDependencyType.SIGNAL) : null;
  }

  /** merge step runtime updates to the runtime summary. */
  public void mergeRuntimeUpdate(
      List<TimelineEvent> pendingTimeline, Map<String, Artifact> pendingArtifacts) {
    if (timeline.addAll(pendingTimeline)) {
      synced = false;
    }
    if (pendingArtifacts != null && !pendingArtifacts.isEmpty()) {
      for (Map.Entry<String, Artifact> entry : pendingArtifacts.entrySet()) {
        String key = entry.getKey();
        if (!entry.getValue().equals(artifacts.get(key))) {
          if (artifacts.containsKey(key)
              && artifacts.get(key).getType() == Artifact.Type.DEFAULT
              && entry.getValue().getType() == Artifact.Type.DEFAULT) {
            artifacts.get(key).asDefault().getData().putAll(entry.getValue().asDefault().getData());
          } else {
            artifacts.put(entry.getKey(), entry.getValue());
          }
          synced = false;
        }
      }
    }
    if (!synced) {
      runtimeState.setModifyTime(System.currentTimeMillis());
    }
  }

  /** clean up after synchronization. */
  public void cleanUp() {
    pendingRecords.clear();
    synced = true;
    dbOperation = DbOperation.UPDATE;
  }

  /** Add an event to the timeline if not identical to the latest event. */
  public void addTimeline(TimelineEvent event) {
    if (timeline.add(event)) {
      synced = false;
    }
  }

  /** update step runtime summary to CREATED status. */
  public void markCreated(MaestroTracingManager tracingManager) {
    runtimeState.setCreateTime(updateStatus(StepInstance.Status.CREATED, tracingManager));
  }

  /** update step runtime summary to INITIALIZED status. */
  public void markInitialized(MaestroTracingManager tracingManager) {
    runtimeState.setInitializeTime(updateStatus(StepInstance.Status.INITIALIZED, tracingManager));
  }

  /** update step runtime summary to INTERNAL_ERROR status. */
  public void markInternalError(Exception e, MaestroTracingManager tracingManager) {
    markTerminated(StepInstance.Status.INTERNALLY_FAILED, null);
    addTimeline(TimelineDetailsEvent.from(Details.create(e, false, "marked as INTERNALLY_FAILED")));
    if (tracingManager != null) {
      // call markTerminated without tracing manager, then finish here with the exception
      tracingManager.handleStepStatus(tracingContext, StepInstance.Status.INTERNALLY_FAILED, e);
    }
  }

  /** update step runtime summary to terminal status. */
  public void markTerminated(
      StepInstance.Status terminalStatus, MaestroTracingManager tracingManager) {
    Checks.checkTrue(
        terminalStatus.isTerminal(),
        "Cannot terminate step %s to a non-terminal state [%s]",
        getIdentity(),
        terminalStatus);
    if (terminalStatus.isRestartable()) {
      stepRetry.setRetryable(false);
    }
    runtimeState.setEndTime(updateStatus(terminalStatus, tracingManager));
    dbOperation = DbOperation.UPSERT; // last db ops to upsert everything
  }

  /** update step runtime summary to PAUSED status. */
  public void markPaused(MaestroTracingManager tracingManager) {
    runtimeState.setPauseTime(updateStatus(StepInstance.Status.PAUSED, tracingManager));
  }

  /** update step runtime summary to WAITING_FOR_SIGNALS status. */
  public void markWaitSignal(MaestroTracingManager tracingManager) {
    runtimeState.setWaitSignalTime(
        updateStatus(StepInstance.Status.WAITING_FOR_SIGNALS, tracingManager));
  }

  /** update step runtime summary to EVALUATING_PARAMS status. */
  public void markEvaluateParam(MaestroTracingManager tracingManager) {
    runtimeState.setEvaluateParamTime(
        updateStatus(StepInstance.Status.EVALUATING_PARAMS, tracingManager));
  }

  /** update step runtime summary to WAITING_FOR_PERMITS status. */
  public void markWaitPermit(MaestroTracingManager tracingManager) {
    runtimeState.setWaitPermitTime(
        updateStatus(StepInstance.Status.WAITING_FOR_PERMITS, tracingManager));
    dbOperation = DbOperation.UPSERT; // After param evaluation, upsert everything to update params
  }

  /** update step runtime summary to STARTING status. */
  public void markStarting(MaestroTracingManager tracingManager) {
    runtimeState.setStartTime(updateStatus(StepInstance.Status.STARTING, tracingManager));
  }

  /** update step runtime summary to RUNNING status. */
  public void markExecuting(MaestroTracingManager tracingManager) {
    runtimeState.setExecuteTime(updateStatus(StepInstance.Status.RUNNING, tracingManager));
  }

  /** update step runtime summary to FINISHING status. */
  public void markFinishing(MaestroTracingManager tracingManager) {
    runtimeState.setFinishTime(updateStatus(StepInstance.Status.FINISHING, tracingManager));
  }

  private long updateStatus(StepInstance.Status nextStatus, MaestroTracingManager tracingManager) {
    long markTime = System.currentTimeMillis();
    pendingRecords.add(
        StepInstanceUpdateJobEvent.createRecord(runtimeState.getStatus(), nextStatus, markTime));
    runtimeState.setStatus(nextStatus);
    runtimeState.setModifyTime(markTime);
    synced = false;
    if (tracingManager != null) {
      tracingManager.handleStepStatus(tracingContext, nextStatus);
    }
    return markTime;
  }

  /** return step runtime identity info. */
  @JsonIgnore
  public String getIdentity() {
    return String.format("[%s][%s][%s]", stepId, stepAttemptId, stepInstanceUuid);
  }

  /**
   * Ignore failure mode only if KILL action is from upstream or KILL action is a workflow level
   * action. In either case, no need to apply failure mode after the step is failed.
   */
  @JsonIgnore
  public void configIgnoreFailureMode(StepAction action, WorkflowSummary summary) {
    ignoreFailureMode =
        action.getAction() == Actions.StepInstanceAction.KILL
            && (action.isWorkflowAction()
                || !(action.getWorkflowId().equals(summary.getWorkflowId())
                    && action.getWorkflowInstanceId() == summary.getWorkflowInstanceId()
                    && action.getWorkflowRunId() == summary.getWorkflowRunId()
                    && action.getStepId().equals(getStepId())));
  }
}
