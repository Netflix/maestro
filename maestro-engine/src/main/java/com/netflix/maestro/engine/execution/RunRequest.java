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

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.RestartPolicy;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Internal data object to hold the data of external run requests (e.g. workflow (re)start or step
 * restart) plus some system injected info. It won't be persisted to DB. Partial of its data will be
 * used to create run config, which will be persisted in the workflow instance DB.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class RunRequest {
  @NonNull private final Initiator initiator;
  private final long requestTime;
  private final UUID requestId;

  @NonNull private RunPolicy currentPolicy; // policy used by the handler to run this request

  @Nullable private final List<Tag> runtimeTags;
  @Nullable private final String correlationId;
  @Nullable private final Long instanceStepConcurrency; // null means unset and disabled
  @Nullable private final Map<String, ParamDefinition> runParams;
  @Nullable private final Map<String, Map<String, ParamDefinition>> stepRunParams;
  @Nullable private final Map<String, Artifact> artifacts;

  @Nullable private RestartConfig restartConfig;
  private final boolean persistFailedRun;

  /** Static util method to extract the last node from the restart path. */
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  public static RestartConfig.RestartNode getCurrentNode(RestartConfig config) {
    Checks.checkTrue(
        config != null && !ObjectHelper.isCollectionEmptyOrNull(config.getRestartPath()),
        "Cannot get restart info in empty restart configuration");
    return config.getRestartPath().get(config.getRestartPath().size() - 1);
  }

  /** Static util method to extract the second to the last node from the restart path. */
  public static Optional<RestartConfig.RestartNode> getNextNode(RestartConfig config) {
    if (config == null) {
      return Optional.empty();
    }
    List<RestartConfig.RestartNode> path = config.getRestartPath();
    if (path != null && path.size() > 1) {
      return Optional.of(path.get(path.size() - 2));
    }
    return Optional.empty();
  }

  /** Get the current restart workflow id. */
  public String getRestartWorkflowId() {
    return getCurrentNode(restartConfig).getWorkflowId();
  }

  /** Get the current restart workflow instance id. */
  public long getRestartInstanceId() {
    return getCurrentNode(restartConfig).getInstanceId();
  }

  /** Get the current restart step id. */
  public String getRestartStepId() {
    return getCurrentNode(restartConfig).getStepId();
  }

  /** Update some info, e.g. adding new restart node to the path. */
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  public void updateForUpstream(String workflowId, long instanceId, @Nullable String stepId) {
    Checks.checkTrue(
        restartConfig != null
            && !ObjectHelper.isCollectionEmptyOrNull(restartConfig.getRestartPath()),
        "Cannot update restart info in empty restart configuration for initiator [%s]",
        initiator);
    restartConfig
        .getRestartPath()
        .add(new RestartConfig.RestartNode(workflowId, instanceId, stepId));
    // Use RESTART_FROM_SPECIFIC (same in updateForDownstreamIfNeeded) along the restart path
    this.currentPolicy = RunPolicy.RESTART_FROM_SPECIFIC;
  }

  /** Update some info only for restart case, e.g. remove last restart node from the path. */
  public void updateForDownstreamIfNeeded(String currentStepId, WorkflowInstance toRestart) {
    if (restartConfig != null) {
      // still along restart path and not reach the downstream
      if (currentStepId.equals(getRestartStepId()) && restartConfig.getRestartPath().size() > 1) {
        restartConfig.getRestartPath().remove(restartConfig.getRestartPath().size() - 1);
        validateIdentity(toRestart);
        if (restartConfig.getRestartPath().size() == 1) {
          this.currentPolicy = restartConfig.getRestartPolicy();
        } else {
          // Use RESTART_FROM_SPECIFIC (same in updateForUpstream) along the restart path
          this.currentPolicy = RunPolicy.RESTART_FROM_SPECIFIC;
        }
        // not along the restart path
        if (initiator.getType() == Initiator.Type.FOREACH
            && getRestartInstanceId() != toRestart.getWorkflowInstanceId()) {
          clearRestartFor(restartConfig.getDownstreamPolicy());
        }
      } else { // not along the restart path or have reached downstream
        clearRestartFor(restartConfig.getDownstreamPolicy());
      }
    }
  }

  /** Update some info to start a new run. */
  @SuppressWarnings({"PMD.NullAssignment"})
  public void clearRestartFor(RunPolicy runPolicy) {
    this.currentPolicy = runPolicy;
    this.restartConfig = null;
  }

  /** Get workflow identity. */
  public String getWorkflowIdentity() {
    RestartConfig.RestartNode node = getCurrentNode(restartConfig);
    return "[" + node.getWorkflowId() + "][" + node.getInstanceId() + "]";
  }

  /** check if this run request is a fresh run (i.e. not a restart). */
  public boolean isFreshRun() {
    return currentPolicy == null || currentPolicy.isFreshRun();
  }

  /** validate the identity. */
  public void validateIdentity(WorkflowInstance toRestart) {
    // instance id will not always match for foreach
    if (initiator.getType() == Initiator.Type.FOREACH) {
      Checks.checkTrue(
          getRestartWorkflowId().equals(toRestart.getWorkflowId()),
          "Cannot restart a FOREACH iteration [%s] as it does not match run request [%s]",
          toRestart.getWorkflowId(),
          getRestartWorkflowId());
    } else {
      Checks.checkTrue(
          getRestartWorkflowId().equals(toRestart.getWorkflowId())
              && getRestartInstanceId() == toRestart.getWorkflowInstanceId(),
          "Cannot restart a workflow instance %s as it does not match run request %s",
          toRestart.getIdentity(),
          getWorkflowIdentity());
    }
  }

  /** check if this run request is a system initiated run. */
  public boolean isSystemInitiatedRun() {
    return !initiator.getType().isRestartable();
  }

  /** Extract info to create a run config object. */
  public RunConfig toRunConfig() {
    RunConfig runConfig = new RunConfig();
    runConfig.setRuntimeTags(this.runtimeTags);
    runConfig.setCorrelationId(this.correlationId);
    runConfig.setPolicy(this.currentPolicy);
    runConfig.setRestartConfig(this.restartConfig);
    return runConfig;
  }

  /** get the requester info. */
  public User getRequester() {
    if (initiator.getType() == Initiator.Type.MANUAL) {
      return ((ManualInitiator) initiator).getUser();
    }
    return null;
  }

  /** builder class for lombok. */
  public static final class RunRequestBuilder {
    /** Builder for currentPolicy transformed from RestartPolicy . */
    public RunRequestBuilder currentPolicy(RestartPolicy input) {
      if (input == null) {
        this.currentPolicy = Defaults.DEFAULT_RESTART_POLICY;
      } else {
        this.currentPolicy = RunPolicy.valueOf(input.name());
      }
      return this;
    }

    /** Builder for currentPolicy. */
    public RunRequestBuilder currentPolicy(RunPolicy input) {
      this.currentPolicy = input;
      return this;
    }

    /** Builder for initiator. */
    public RunRequestBuilder requester(User user) {
      ManualInitiator initiator = new ManualInitiator();
      initiator.setDepth(0);
      initiator.setUser(user);
      this.initiator = initiator;
      return this;
    }
  }
}
