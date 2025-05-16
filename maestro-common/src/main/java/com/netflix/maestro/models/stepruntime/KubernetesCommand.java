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
package com.netflix.maestro.models.stepruntime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Kubernetes batch job command. */
@JsonDeserialize(builder = KubernetesCommand.KubernetesCommandBuilder.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "app_name",
      "cpu",
      "disk",
      "gpu",
      "memory",
      "image",
      "entrypoint",
      "env",
      "job_deduplication_key",
      "owner_email"
    },
    alphabetic = true)
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
public class KubernetesCommand {
  private final String appName;
  private final String cpu;
  private final String disk;
  private final String gpu;
  private final String memory;
  private final String image;
  private final String entrypoint;
  private final Map<String, String> env;
  private final String jobDeduplicationKey;
  private final String ownerEmail;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class KubernetesCommandBuilder {}
}
