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
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.RetryPolicy;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.validations.TagListConstraint;
import java.util.Locale;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.Getter;

/** Step instance data model. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id",
      "workflow_uuid",
      "step_uuid",
      "correlation_id",
      "step_instance_id",
      "workflow_version_id",
      "owner",
      "definition",
      "tags",
      "params",
      "transition",
      "step_retry",
      "timeout_in_millis",
      "runtime_state",
      "dependencies",
      "outputs",
      "artifacts",
      "timeline"
    },
    alphabetic = true)
@Data
public class StepInstance {
  @NotNull private String workflowId;

  @Min(1)
  private long workflowInstanceId;

  @Min(1)
  private long workflowRunId;

  @NotNull private String stepId;

  @Min(1)
  private long stepAttemptId;

  @NotNull private String workflowUuid;
  @NotNull private String stepUuid; // internal UUID to identify this step run
  @NotNull private String correlationId; // default to the workflow instance uuid

  @Min(1)
  private long stepInstanceId; // step sequence number.

  @Min(1)
  private long workflowVersionId; // version id of baseline workflow

  @NotNull private Long groupId; // the group id to group a set of root workflows together

  // required owner from workflow instance properties snapshot.
  @Valid @NotNull private User owner;

  @Valid @NotNull private Step definition;

  @Valid @TagListConstraint private TagList tags;
  @Valid private Map<String, Parameter> params; // might change later due to output param

  /** set and wrap step params. */
  public void setParams(Map<String, Parameter> input) {
    this.params = Parameter.preprocessInstanceParams(input);
  }

  @Valid private StepInstanceTransition transition; // might change later due to output param
  @Valid private StepInstance.StepRetry stepRetry;

  @Nullable private Long timeoutInMillis; // parsed timeout for a given step instance

  @Valid @NotNull private StepRuntimeState runtimeState;

  @Valid private Map<StepDependencyType, StepDependencies> dependencies;
  @Valid private Map<StepOutputsDefinition.StepOutputType, StepOutputs> outputs;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private Map<String, Artifact> artifacts;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Valid
  private Timeline timeline;

  @Nullable private Map<String, ParamDefinition> stepRunParams;
  @Nullable private RestartConfig restartConfig;

  /** Enrich step instance data for external API endpoints. */
  @JsonIgnore
  public void enrich() {
    if (timeline != null) {
      timeline.enrich(this);
    }
  }

  /** Get step instance identity. */
  @JsonIgnore
  public String getIdentity() {
    return String.format("[%s][%s][%s][%s]", workflowId, workflowInstanceId, workflowRunId, stepId);
  }

  @JsonIgnore
  public StepDependencies getSignalDependencies() {
    return dependencies != null ? dependencies.get(StepDependencyType.SIGNAL) : null;
  }

  /** step retry info. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "error_retries",
        "error_retry_limit",
        "platform_retries",
        "platform_retry_limit",
        "timeout_retries",
        "timeout_retry_limit",
        "manual_retries",
        "retryable",
        "backoff"
      },
      alphabetic = true)
  @Data
  public static class StepRetry {
    private long errorRetries; // retry count due to user error
    private long errorRetryLimit;
    private long platformRetries; // retry count due to platform failure
    private long platformRetryLimit;
    private long timeoutRetries; // retry count due to timeout failure
    private long timeoutRetryLimit;
    private long manualRetries; // retry count due to manual api call to restart
    private boolean retryable = true; // mark if the step is retryable by the system
    private RetryPolicy.Backoff backoff;

    /** check if reaching error retry limit. */
    public boolean hasReachedErrorRetryLimit() {
      return errorRetries >= errorRetryLimit || !retryable;
    }

    /** check if reaching platform retry limit. */
    public boolean hasReachedPlatformRetryLimit() {
      return platformRetries >= platformRetryLimit || !retryable;
    }

    /** check if reaching timeout retry limit. */
    public boolean hasReachedTimeoutRetryLimit() {
      return timeoutRetries >= timeoutRetryLimit || !retryable;
    }

    /** increment corresponding retry count based on status. */
    public void incrementByStatus(Status status) {
      if (status == Status.USER_FAILED) {
        errorRetries++;
      } else if (status == Status.PLATFORM_FAILED) {
        platformRetries++;
      } else if (status == Status.TIMEOUT_FAILED) {
        timeoutRetries++;
      } else if (status.isRestartable()) {
        manualRetries++;
      } else {
        throw new MaestroInvalidStatusException(
            "Invalid status [%s] to increase retry count", status);
      }
    }

    /**
     * Get next retry delay based on error and configured retry policy.
     *
     * @param status the step instance status
     * @return the next retry delay in secs.
     */
    public int getNextRetryDelay(Status status) {
      if (status == Status.USER_FAILED) {
        return backoff.getNextRetryDelayForUserError(errorRetries);
      } else if (status == Status.PLATFORM_FAILED) {
        return backoff.getNextRetryDelayForPlatformError(platformRetries);
      } else if (status == Status.TIMEOUT_FAILED) {
        return backoff.getNextRetryDelayForTimeoutError(timeoutRetries);
      } else {
        // Not expected to get retry delay for any other errors.
        throw new MaestroInvalidStatusException(
            "Invalid status [%s] to get next retry delay", status);
      }
    }

    /** create step retry object based on retry policy from step definition. */
    public static StepRetry from(@Nullable RetryPolicy policy) {
      RetryPolicy retryPolicy = RetryPolicy.tryMergeWithDefault(policy);
      StepRetry stepRetry = new StepRetry();
      stepRetry.errorRetryLimit = retryPolicy.getErrorRetryLimit();
      stepRetry.platformRetryLimit = retryPolicy.getPlatformRetryLimit();
      stepRetry.timeoutRetryLimit = retryPolicy.getTimeoutRetryLimit();
      stepRetry.retryable = true;
      stepRetry.backoff = retryPolicy.getBackoff();
      return stepRetry;
    }
  }

  /** Step status. */
  @Getter
  public enum Status {
    /** Step is not created and started yet. */
    NOT_CREATED(false, false, false, false),
    /** Step instance is started. */
    CREATED(false, false, false, false),
    /** Step is initialized, e.g. params are evaluated. */
    INITIALIZED(false, false, false, false),

    /** Step is waiting for releasing breakpoint; an optional state. */
    PAUSED(false, false, true, false),

    /** Step is waiting for signals. */
    WAITING_FOR_SIGNALS(false, false, true, false),
    /** Step is evaluating params. */
    EVALUATING_PARAMS(false, false, true, false),
    /** Step is waiting for tag permits. */
    WAITING_FOR_PERMITS(false, false, true, false),

    /** Step starts and initialize run step runtime. */
    STARTING(false, false, false, false),

    /** Step is running after initialization. */
    RUNNING(false, false, true, false),

    /** Step is finishing after execution. */
    FINISHING(false, false, false, false),

    /** Step is disabled at workflow instance start time, terminal state. */
    DISABLED(true, true, false, false),
    /**
     * Step should not run and user logic does not run. Maestro runs over this step when its if
     * condition is false or the workflow is already failed when failure mode is FAIL_AFTER_RUNNING.
     * Users can discard steps with this status. terminal state.
     */
    UNSATISFIED(true, true, false, false),
    /** Step is skipped by users at runtime, terminal state. */
    SKIPPED(true, true, false, false),
    /** Step is succeeded, terminal state. */
    SUCCEEDED(true, true, false, false),
    /**
     * Step is completed with error after reaching retry limit with IGNORE_FAILURE mode, terminal
     * state.
     */
    COMPLETED_WITH_ERROR(true, true, false, false),

    /** Step is failed due to user execution error, terminal state. */
    USER_FAILED(true, false, true, true),
    /** Step is failed due to platform execution error, terminal state. */
    PLATFORM_FAILED(true, false, true, true),
    /** Step is fatally failed and cause workflow to be failed, terminal state. */
    FATALLY_FAILED(true, false, true, false),
    /**
     * Step is failed due to scheduler internal error and cause workflow to be immediately failed
     * without retry and ignore failure mode, terminal state.
     */
    INTERNALLY_FAILED(true, false, true, false),

    /** Step is stopped by a user or the workflow, terminal state. */
    STOPPED(true, false, false, false),
    /** Step is failed due to execution timeout error, terminal state. */
    TIMEOUT_FAILED(true, false, true, true),
    /** Step is fatally timed out by the system, terminal state. */
    TIMED_OUT(true, false, false, false);

    @JsonIgnore private final boolean terminal; // if it is terminal
    @JsonIgnore private final boolean complete; // if it is complete done

    @JsonIgnore private final boolean overview; // if step reference should be kept in rollup

    @JsonIgnore private final boolean retryable; // if system will automatically retry

    @JsonIgnore
    public boolean isRestartable() {
      return terminal && !complete && !retryable;
    }

    @JsonIgnore
    public boolean shouldWakeup() {
      return !terminal || retryable;
    }

    Status(boolean terminal, boolean complete, boolean overview, boolean retryable) {
      this.terminal = terminal;
      this.complete = complete;
      this.overview = overview;
      this.retryable = retryable;
    }

    /** Static creator. */
    @JsonCreator
    public static Status create(@NotNull String status) {
      return Status.valueOf(status.toUpperCase(Locale.US));
    }
  }
}
