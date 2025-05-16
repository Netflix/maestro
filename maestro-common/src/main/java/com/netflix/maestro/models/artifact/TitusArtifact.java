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
package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.stepruntime.TitusCommand;
import com.netflix.maestro.models.timeline.TimelineMilestoneEvent;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.ToString;

/** Titus artifact to store compute information. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "cmb_job_id",
      "cmb_ui_link",
      "execution_script",
      "titus_job_id",
      "titus_log_ui_link",
      "titus_execution_output",
      "titus_execution_error_output",
      "titus_options",
      "titus_task_id",
      "milestones"
    },
    alphabetic = true)
@Data
@ToString
public class TitusArtifact implements Artifact {
  private String cmbJobId;
  private String cmbUiLink;
  private String executionScript;
  private String titusJobId;
  private String titusLogUiLink;
  private String titusExecutionOutput;
  private String titusExecutionErrorOutput;
  private TitusCommand titusOptions;
  private String titusTaskId;
  private List<TimelineMilestoneEvent> milestones = new ArrayList<>();

  @JsonIgnore
  @Override
  public TitusArtifact asTitus() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.TITUS;
  }
}
