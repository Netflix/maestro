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
package com.netflix.maestro.engine.steps;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Step runtime implementation. It is expected to be stateless and thread safe.
 *
 * <p>The state should not be kept in instance variables within StepRuntime.
 *
 * <p>The whole execution offers at-least-once guarantee. Therefore, the logic implemented here
 * should be idempotent. For example, Periodically check sleep time or check the container status.
 */
public interface StepRuntime {
  /** Maestro system user. */
  User SYSTEM_USER = User.create(Constants.MAESTRO_QUALIFIER);

  /**
   * Step runtime result.
   *
   * <p>Step runtime can use nextPollingDelayInMillis to control the polling interval for the next
   * call. Note that this is just a hint. The workflow engine may choose to ignore it, e.g. using a
   * different delay during retry backoff.
   *
   * @param state state of the step runtime.
   * @param artifacts artifacts to persist
   * @param timeline timeline to persist
   * @param nextPollingDelayInMillis polling interval for the next execute() call.
   */
  record Result(
      @NotNull State state,
      Map<String, Artifact> artifacts,
      List<TimelineEvent> timeline,
      @Nullable Long nextPollingDelayInMillis) {

    public Result(
        @NotNull State state, Map<String, Artifact> artifacts, List<TimelineEvent> timeline) {
      this(state, artifacts, timeline, null);
    }

    public static Result of(State state) {
      return new Result(state, new LinkedHashMap<>(), new ArrayList<>());
    }

    public static Result of(State state, Long nextPollingDelayInMillis) {
      return new Result(state, new LinkedHashMap<>(), new ArrayList<>(), nextPollingDelayInMillis);
    }

    public boolean shouldPersist() {
      return (artifacts != null && !artifacts.isEmpty())
          || (timeline != null && !timeline.isEmpty());
    }
  }

  /** step runtime state. */
  enum State {
    /** continue with the current state if execute(); move to the next state if start(). */
    CONTINUE,
    /** done with the current state, move to the next step state. */
    DONE,
    /** the step becomes USER_ERROR terminate state due to user error. */
    USER_ERROR,
    /** the step becomes PLATFORM_ERROR terminate state due to platform error. */
    PLATFORM_ERROR,
    /** the step becomes FATAL_ERROR terminate state. */
    FATAL_ERROR,
    /** the step becomes STOPPED terminate state. */
    STOPPED,
    /** the step becomes TIMED_OUT terminate state. */
    TIMED_OUT;

    public boolean isFailed() {
      // Note that TIMED_OUT is currently considered as failed.
      return this != CONTINUE && this != DONE && this != STOPPED;
    }

    public boolean isTerminal() {
      return this != CONTINUE;
    }
  }

  /**
   * Logic to run when starting the maestro step runtime.
   *
   * <p>With the at-least-once guarantee, this method will run at the beginning of the life cycle of
   * a step instance run. However, there is a chance to run multiple times during failover. So the
   * logic here should be idempotent. Return result includes new step state, new artifact changes,
   * and new timeline info to persist.
   *
   * <p>The input data are a copy of the original summary data. Any changes on them will be
   * discarded once the call is returned. Please include the return changes in Result object.
   *
   * <p>If an exception is thrown during starting, the step will fail fatally and then the whole
   * workflow fails without retrying.
   *
   * @param workflowSummary workflow instance runtime summary. It should be treated as read-only.
   *     Any changes on it will not be persisted.
   * @param runtimeSummary step instance runtime summary. It should be treated as read-only. Any
   *     changes on it will not be persisted.
   * @return Result includes new step state, new artifact changes, and new timeline info to persist.
   * @throws RuntimeException no expected checked exception. if any exception is thrown and the step
   *     will fail and the workflow run will fail
   */
  default Result start(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    return Result.of(State.DONE);
  }

  /**
   * Customized step execution logic.
   *
   * <p>While the step status is RUNNING, the code in execute() will be called periodically with a
   * preset polling interval. Additionally, if the execution throws an exception, the execution will
   * be retried as another step instance run.
   *
   * <p>The input data are a copy of the original summary data. Any changes on them will be
   * discarded once the call is returned. Please include the return changes in Result object.
   *
   * <p>If an exception is thrown during starting, the step will fail fatally and then the whole
   * workflow fails without retrying.
   *
   * @param workflowSummary workflow instance runtime summary. It should be treated as read-only.
   *     Any changes on it will not be persisted.
   * @param runtimeSummary step instance runtime summary. It should be treated as read-only. Any
   *     changes on it will not be persisted.
   * @return Result includes new step state, new artifact changes, and new timeline info to persist.
   * @throws RuntimeException no expected checked exception. if any exception is thrown and the step
   *     will fail and the workflow run will fail
   */
  default Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    return Result.of(State.DONE);
  }

  /**
   * logic to run when a running step is terminated by the workflow, which is entering a terminal
   * state. It will be called by workflow engine instead of by step execution logic.
   *
   * <p>The input data are a copy of the original summary data. Any changes on them will be
   * discarded once the call is returned. No return is expected to terminate method call.
   *
   * @param workflowSummary workflow instance runtime summary. It should be treated as read-only.
   *     Any changes on it will not be persisted.
   * @param runtimeSummary step instance runtime summary. It should be treated as read-only. Any
   *     changes on it will not be persisted.
   * @return Result includes new step state, new artifact changes, and new timeline info to persist.
   * @throws RuntimeException Exceptions thrown in this method are expected to be retryable. If any
   *     exception is thrown, terminate method will be retried again later. If the exception is not
   *     retryable, should log the error and metric and then swallow the exception and continue to
   *     clean up the step runtime.
   */
  default Result terminate(WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    return Result.of(State.STOPPED);
  }

  /**
   * Inject runtime parameters based on the step definition data.
   *
   * @return a collections of runtime generated parameters to inject
   * @param workflowSummary workflow summary
   * @param step step definition
   */
  default Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    return Collections.emptyMap();
  }

  /**
   * Inject runtime tags from the step runtime.
   *
   * @return a collections of runtime generated tags to inject
   */
  default List<Tag> injectRuntimeTags() {
    return Collections.emptyList();
  }
}
