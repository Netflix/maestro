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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Defaults;
import javax.validation.constraints.NotNull;
import lombok.Data;

/** Step instance runtime state, it includes the status and timestamps info at runtime. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "status",
      "create_time",
      "initialize_time",
      "pause_time",
      "wait_signal_time",
      "evaluate_param_time",
      "wait_permit_time",
      "start_time",
      "execute_time",
      "finish_time",
      "end_time",
      "modify_time"
    },
    alphabetic = true)
@Data
public class StepRuntimeState {
  @NotNull private StepInstance.Status status = Defaults.DEFAULT_STEP_INSTANCE_INITIAL_STATUS;
  private Long createTime;
  private Long initializeTime;
  private Long pauseTime;
  private Long waitSignalTime;
  private Long evaluateParamTime;
  private Long waitPermitTime;
  private Long startTime;
  private Long executeTime;
  private Long finishTime;
  private Long endTime;
  private Long modifyTime;
}
