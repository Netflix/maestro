/*
 * Copyright 2025 Netflix, Inc.
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
import com.netflix.maestro.models.stepruntime.KubernetesCommand;
import lombok.Data;
import lombok.ToString;

/** Kubernetes artifact to store compute information. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"job_id", "execution_script", "execution_output", "command"},
    alphabetic = true)
@Data
@ToString
public class KubernetesArtifact implements Artifact {
  private String jobId;
  private String executionScript;
  private String executionOutput;
  private KubernetesCommand command;

  @JsonIgnore
  @Override
  public KubernetesArtifact asKubernetes() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.KUBERNETES;
  }
}
