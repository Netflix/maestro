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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.RestartPolicy;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Setting info only for workflow instance or step restart. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "restart_path",
      "restart_policy",
      "downstream_policy",
      "restart_params",
      "step_restart_params",
      "skip_steps"
    },
    alphabetic = true)
@JsonDeserialize(builder = RestartConfig.RestartConfigBuilder.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class RestartConfig {
  /**
   * restartPath records the restart chain in a reverse order for a nested workflow restart case,
   * where the last node is the one currently to be restarted.
   */
  private final List<RestartNode> restartPath;

  /**
   * run policy for the handler used to run it. For step restart, it will always be
   * RESTART_FROM_SPECIFIC.
   */
  private final RunPolicy restartPolicy;

  /** run policy to run its downstream. */
  private final RunPolicy downstreamPolicy;

  // optional param changes from restart request
  @Nullable private final Map<String, ParamDefinition> restartParams;
  // stepId to <param name : param definition>
  @Nullable private final Map<String, Map<String, ParamDefinition>> stepRestartParams;
  @Nullable private final Set<String> skipSteps;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class RestartConfigBuilder {

    /** build method with default values. */
    public RestartConfig build() {
      if (restartPath == null) {
        restartPath = new ArrayList<>();
      }
      ParamDefinition.preprocessDefinitionParams(restartParams);
      if (stepRestartParams != null) {
        stepRestartParams.values().forEach(ParamDefinition::preprocessDefinitionParams);
      }

      if (restartPolicy == null) {
        restartPolicy = Defaults.DEFAULT_RESTART_POLICY;
      }
      if (downstreamPolicy == null) {
        downstreamPolicy = Defaults.DEFAULT_RESTART_POLICY;
      }

      return new RestartConfig(
          restartPath,
          restartPolicy,
          downstreamPolicy,
          restartParams,
          stepRestartParams,
          skipSteps);
    }

    /** add a restart node to restart path. */
    public RestartConfigBuilder addRestartNode(
        String workflowId, long instanceId, @Nullable String stepId) {
      if (restartPath == null) {
        restartPath = new ArrayList<>();
      }
      RestartNode node = new RestartNode(workflowId, instanceId, stepId);
      restartPath.add(node);
      return this;
    }

    /** build restartPolicy. */
    public RestartConfigBuilder setRestartPolicy(RestartPolicy input) {
      if (input != null) {
        this.restartPolicy = RunPolicy.valueOf(input.name());
      }
      return this;
    }

    /** build downstreamPolicy. */
    public RestartConfigBuilder setDownstreamPolicy(RestartPolicy input) {
      if (input != null) {
        this.downstreamPolicy = RunPolicy.valueOf(input.name());
      }
      return this;
    }
  }

  /** Data model to keep the workflow and step info along the restart path. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {"workflow_id", "instance_id", "step_id"},
      alphabetic = true)
  @Getter
  @ToString
  @EqualsAndHashCode
  public static class RestartNode {
    private final String workflowId;
    private final long instanceId;
    @Nullable private final String stepId;

    /** Constructor. */
    @JsonCreator
    public RestartNode(
        @JsonProperty("workflow_id") String workflowId,
        @JsonProperty("instance_id") long instanceId,
        @Nullable @JsonProperty("step_id") String stepId) {
      this.workflowId = workflowId;
      this.instanceId = instanceId;
      this.stepId = stepId;
    }
  }
}
