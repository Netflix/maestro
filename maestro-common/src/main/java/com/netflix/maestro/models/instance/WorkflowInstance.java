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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.Timeline;
import java.util.Locale;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.Getter;

/** Workflow instance data model. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "workflow_uuid",
      "correlation_id",
      "workflow_version_id",
      "internal_id",
      "execution_id",
      "run_config",
      "run_params",
      "step_run_params",
      "run_properties",
      "runtime_workflow",
      "runtime_dag",
      "timeout_in_millis",
      "params",
      "initiator",
      "status",
      "request_time",
      "create_time",
      "start_time",
      "end_time",
      "modify_time",
      "runtime_overview",
      "artifacts",
      "timeline",
      "aggregated_info"
    },
    alphabetic = true)
@Data
public class WorkflowInstance {
  @Valid @NotNull private String workflowId;

  @Min(1)
  private long workflowInstanceId;

  @Min(1)
  private long workflowRunId;

  @NotNull private String workflowUuid; // uuid for dedup

  private String correlationId;

  private Long internalId; // unique workflow internal id to identify this workflow

  @Min(1)
  private long workflowVersionId; // version id of baseline workflow

  private String executionId; // internal execution id to identify this workflow run

  // runtime config to tailor the baseline workflow definition
  @Valid private RunConfig runConfig;

  @Valid private Map<String, ParamDefinition> runParams; // runtime parameter overrides

  @Valid
  // step runtime parameter overrides
  private Map<String, Map<String, ParamDefinition>> stepRunParams;

  /** set and wrap run params. */
  public void setRunParams(Map<String, ParamDefinition> input) {
    this.runParams = ParamDefinition.preprocessDefinitionParams(input);
  }

  /** set and wrap step run params. */
  public void setStepRunParams(Map<String, Map<String, ParamDefinition>> inputs) {
    if (inputs != null) {
      inputs.values().forEach(ParamDefinition::preprocessDefinitionParams);
    }
    this.stepRunParams = inputs;
  }

  // required properties snapshot when the workflow instance starts.
  @Valid @NotNull private RunProperties runProperties;

  // runtime workflow definition with all overrides
  @Valid @NotNull private Workflow runtimeWorkflow;

  /**
   * runtime dag includes all possible nodes this run can reach. If all succeeds, the instance
   * succeeds. Note that not all nodes in runtime dag will be initially started.
   */
  @Valid @NotNull private Map<String, StepTransition> runtimeDag;

  @Nullable private Long timeoutInMillis; // parsed timeout for a given workflow instance

  // keep the evaluated param results
  @Valid private Map<String, Parameter> params;

  /** set and wrap workflow params. */
  public void setParams(Map<String, Parameter> input) {
    this.params = Parameter.preprocessInstanceParams(input);
  }

  @Valid @NotNull private Initiator initiator;

  /** Workflow instance status, which is an aggregated status across instance runs. */
  @Valid @NotNull private WorkflowInstance.Status status;

  /** When maestro workflow run is requested by callers. */
  private Long requestTime;

  /** When the instance is created/initiated in maestro. */
  private Long createTime;

  /**
   * When the instance is starting after run strategy check, equals to the startTime of the first
   * step (i.e. default start step).
   */
  private Long startTime;

  /**
   * When the instance is in a terminal state, equals to the endTime of all ended steps, excluding
   * default end step.
   */
  private Long endTime;

  /** When the workflow instance is lastly modified. */
  private Long modifyTime;

  // embedded step info and statistics and rollup leaf step info and statistics
  @Valid private WorkflowRuntimeOverview runtimeOverview;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private Map<String, Artifact> artifacts;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Valid
  private Timeline timeline;

  /** Aggregated base from previous instance. */
  private WorkflowInstanceAggregatedInfo aggregatedInfo;

  /** Get run status of this workflow instance run. */
  @JsonIgnore
  public WorkflowInstance.Status getRunStatus() {
    if (runtimeOverview == null || runtimeOverview.getRunStatus() == null) {
      return status;
    }
    return runtimeOverview.getRunStatus();
  }

  /** Workflow instance status. */
  public enum Status {
    /**
     * Workflow instance is created with overrides and params evaluated and queued before running.
     */
    CREATED(false),

    /** Workflow instance started to run after run strategy check. */
    IN_PROGRESS(false),
    /** Workflow instance is paused during running. todo support it. */
    PAUSED(false),

    /** Workflow instance is timed out, a terminal state and immutable. */
    TIMED_OUT(true),
    /** Workflow instance is stopped, a terminal state and immutable. */
    STOPPED(true),

    /**
     * Workflow instance run is failed, a terminal state. To support strict sequential run strategy,
     * raw DB status string might be FAILED or FAILED_1, or FAILED_2 to differentiate among the last
     * run failure, non-last run failure, or user flagged unblocking failure. So it means the raw DB
     * status might be mutated to after the instance is in FAILED terminal state. But those failed
     * states are just for internal usage and won't be exposed to the public data model. All will be
     * parsed to FAILED state. There won't be any instance update events emitted for that as well.
     */
    FAILED(true),

    /** Workflow instance is succeeded, a terminal state. */
    SUCCEEDED(true);

    @JsonIgnore @Getter private final boolean terminal;

    Status(boolean terminal) {
      this.terminal = terminal;
    }

    /** Static creator. */
    @JsonCreator
    public static Status create(@NotNull String wis) {
      if (wis.startsWith(FAILED.name())) {
        return FAILED;
      }
      return Status.valueOf(wis.toUpperCase(Locale.US));
    }
  }

  /** Set correlation id if it is null. */
  @JsonIgnore
  public void fillCorrelationIdIfNull() {
    if (correlationId == null) {
      correlationId = String.format("%s-%s-%s", workflowId, workflowInstanceId, workflowRunId);
    }
  }

  /** Get workflow instance identity. */
  @JsonIgnore
  public String getIdentity() {
    return String.format("[%s][%s][%s]", workflowId, workflowInstanceId, workflowRunId);
  }

  /** Enrich step instance data for external API endpoints. */
  @JsonIgnore
  public void enrich() {
    if (timeline != null) {
      timeline.enrich(this);
    }
  }

  /** check if this instance is a fresh run. */
  @JsonIgnore
  public boolean isFreshRun() {
    return runConfig == null || runConfig.getPolicy() == null || runConfig.getPolicy().isFreshRun();
  }

  @JsonIgnore
  public boolean isInlineWorkflow() {
    return initiator.getType().isInline();
  }
}
