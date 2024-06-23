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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.definition.AccessControl;
import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.validations.PropertiesConstraint;
import javax.validation.Valid;
import lombok.Data;

/** Request to update workflow properties. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "properties",
      "reset_run_strategy_rule",
      "reset_workflow_concurrency",
      "reset_step_concurrency",
    },
    alphabetic = true)
@Data
public class WorkflowPropertiesUpdateRequest {
  @Valid @PropertiesConstraint private Properties properties;

  private boolean resetRunStrategyRule;
  private boolean resetWorkflowConcurrency;
  private boolean resetStepConcurrency;

  @JsonCreator
  public WorkflowPropertiesUpdateRequest(
      @JsonProperty("owner") User owner,
      @JsonProperty("access_control") AccessControl accessControl,
      @JsonProperty("run_strategy") RunStrategy runStrategy,
      @JsonProperty("step_concurrency") Long stepConcurrency,
      @JsonProperty("alerting") Alerting alerting,
      @JsonProperty("alerting_disabled") Boolean alertingDisabled,
      @JsonProperty("signal_trigger_disabled") Boolean signalTriggerDisabled,
      @JsonProperty("time_trigger_disabled") Boolean timeTriggerDisabled,
      @JsonProperty("description") String description,
      @JsonProperty("reset_run_strategy_rule") Boolean resetRunStrategyRule,
      @JsonProperty("reset_workflow_concurrency") Boolean resetWorkflowConcurrency,
      @JsonProperty("reset_step_concurrency") Boolean resetStepConcurrency) {
    this.properties = new Properties();
    properties.setOwner(owner);
    properties.setAccessControl(accessControl);
    properties.setRunStrategy(runStrategy);
    properties.setStepConcurrency(stepConcurrency);
    properties.setAlerting(alerting);
    properties.setAlertingDisabled(alertingDisabled);
    properties.setSignalTriggerDisabled(signalTriggerDisabled);
    properties.setTimeTriggerDisabled(timeTriggerDisabled);
    properties.setDescription(description);

    if (resetRunStrategyRule != null) {
      this.setResetRunStrategyRule(resetRunStrategyRule);
    }
    if (resetWorkflowConcurrency != null) {
      this.setResetWorkflowConcurrency(resetWorkflowConcurrency);
    }
    if (resetStepConcurrency != null) {
      this.setResetStepConcurrency(resetStepConcurrency);
    }

    // post deserialization validation
    if ((this.resetStepConcurrency && stepConcurrency != null)
        || ((this.resetRunStrategyRule || this.resetWorkflowConcurrency) && runStrategy != null)) {
      throw new MaestroValidationException(
          "Setting and resetting the same workflow property in one request is not allowed");
    }
  }
}
