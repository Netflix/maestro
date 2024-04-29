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
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.Criticality;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.validations.TagListConstraint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Workflow instance summary created just before the start of execution, it includes the necessary
 * info (e.g. evaluated workflow parameters and injected workflow tags) needed at runtime.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "internal_id",
      "workflow_version_id",
      "workflow_name",
      "workflow_instance_id",
      "workflow_run_id",
      "correlation_id",
      "creation_time",
      "workflow_uuid",
      "run_policy",
      "restart_config",
      "run_properties",
      "initiator",
      "params",
      "tags",
      "runtime_dag",
      "criticality",
      "instance_step_concurrency"
    },
    alphabetic = true)
@Data
public class WorkflowSummary {
  @NotNull private String workflowId;
  private Long internalId;

  @Min(1)
  private long workflowVersionId;

  private String workflowName;
  private long workflowInstanceId;
  private long workflowRunId;
  private String correlationId;
  private Long creationTime;
  @NotNull private String workflowUuid;

  @NotNull private RunPolicy runPolicy; // the policy used to run to this workflow instance
  @Nullable private RestartConfig restartConfig;

  // from workflow instance properties snapshot
  @Valid @NotNull private RunProperties runProperties;

  @Valid @NotNull private Initiator initiator;

  @Valid private Map<String, Parameter> params;

  @Valid private Map<String, Map<String, ParamDefinition>> stepRunParams;

  /** set and wrap params. */
  public void setParams(Map<String, Parameter> input) {
    this.params = Parameter.preprocessInstanceParams(input);
  }

  @Valid @TagListConstraint private TagList tags;

  private Map<String, StepTransition> runtimeDag; // actual dag used.

  @Nullable private Criticality criticality;

  // instance step concurrency value from its runtime workflow definition
  private Long instanceStepConcurrency; // null means unset and disabled

  @JsonIgnore
  public String getIdentity() {
    return String.format("[%s][%s][%s]", workflowId, workflowInstanceId, workflowRunId);
  }

  @JsonIgnore
  public boolean isFreshRun() {
    return runPolicy == null || runPolicy.isFreshRun();
  }

  /** Get the total number of steps in runtime dag. */
  @JsonIgnore
  public int getTotalStepCount() {
    return runtimeDag == null ? 0 : runtimeDag.size();
  }

  /**
   * It derives the actual instance_step_concurrency based on what users set and system defaults. It
   * needs to consider all the cases and also set the right values to avoid using unnecessary system
   * resources.
   */
  @JsonIgnore
  public long deriveInstanceStepConcurrency() {
    Long stepConcurrency = runProperties.getStepConcurrency();
    // if both are set, should return the smaller one
    if (instanceStepConcurrency != null && stepConcurrency != null) {
      return Math.min(instanceStepConcurrency, stepConcurrency);
    }
    return ObjectHelper.valueOrDefault(
        ObjectHelper.valueOrDefault(instanceStepConcurrency, stepConcurrency),
        Defaults.DEFAULT_STEP_CONCURRENCY);
  }

  /**
   * It derives the runtime tags with their permits based on workflow level and step level
   * step_concurrency and instance_step_concurrency when either enabled or disabled.
   *
   * @return a list of optional runtime tags with permits
   */
  @JsonIgnore
  public List<Tag> deriveRuntimeTagPermits(StepRuntimeSummary runtimeSummary) {
    List<Tag> runtimeTagPermits = new ArrayList<>();
    Long stepConcurrency = runProperties.getStepConcurrency();
    if (instanceStepConcurrency != null) { // instance_step_concurrency is enabled.
      // if step_concurrency is set, then set a tag permit for it
      if (stepConcurrency != null) {
        runtimeTagPermits.add(buildATag(workflowId, stepConcurrency));
      }
      // only if instance_step_concurrency is set for a leaf step, then add a tag permit for it.
      if (runtimeSummary.getType().isLeaf()) {
        runtimeTagPermits.add(buildATag(correlationId, instanceStepConcurrency));
      }
    } else { // instance_step_concurrency is disabled.
      // if step_concurrency is unset, use the default, then set a tag permit for it
      if (stepConcurrency == null) {
        stepConcurrency = Defaults.DEFAULT_STEP_CONCURRENCY;
      }
      runtimeTagPermits.add(buildATag(workflowId, stepConcurrency));
      // always add a default step level step concurrency limit with the default value.
      runtimeTagPermits.add(
          buildATag(
              workflowId + ":" + runtimeSummary.getStepId(), Defaults.DEFAULT_STEP_CONCURRENCY));
    }

    return runtimeTagPermits;
  }

  private Tag buildATag(String name, long permit) {
    Tag newTag = new Tag();
    newTag.setName(Constants.MAESTRO_PREFIX + name);
    newTag.setPermit((int) permit);
    return newTag;
  }
}
