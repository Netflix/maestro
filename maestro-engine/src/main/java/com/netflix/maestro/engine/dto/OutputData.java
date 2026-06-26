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
package com.netflix.maestro.engine.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** OutputData wrapper DTO. */
@Getter
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonDeserialize(builder = OutputData.OutputDataBuilder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "external_job_type",
      "external_job_id",
      "workflow_id",
      "params",
      "artifacts",
      "create_time",
      "modify_time"
    })
@AllArgsConstructor
@ToString
@Builder
public class OutputData {
  @Setter private StepType externalJobType;
  @Setter private String externalJobId;
  @Setter private String workflowId;
  @Setter private Long createTime;
  @Setter private Long modifyTime;
  private final Map<String, Parameter> params;
  private final Map<String, Artifact> artifacts;

  /** Constructor. */
  public OutputData(Map<String, Parameter> params, Map<String, Artifact> artifacts) {
    this.params = params;
    this.artifacts = artifacts;
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class OutputDataBuilder {}

  @JsonIgnore
  public boolean isNotEmpty() {
    return (params != null && !params.isEmpty()) || (artifacts != null && !artifacts.isEmpty());
  }
}
