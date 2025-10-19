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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.GitInfo;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.utils.Checks;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;

/** Job template API request to create a new job template version. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"owner", "status", "support", "test_workflows", "git_info", "definition"},
    alphabetic = true)
@Data
public class JobTemplateCreateRequest {
  /** reserved fields cannot be set within extraInfo. */
  private static final Set<String> RESERVED_FIELDS =
      new HashSet<>(
          Arrays.asList("owner", "status", "support", "test_workflows", "git_info", "definition"));

  @NotNull private User owner;
  @NotNull private String status;
  @NotNull private String support;
  private List<String> testWorkflows;
  private GitInfo gitInfo;

  @Valid @NotNull private JobTemplate.Definition definition;

  private Map<String, Object> extraInfo = new LinkedHashMap<>();

  /** set extra info with validation. */
  public void setExtraInfo(Map<String, Object> extraInfo) {
    if (extraInfo != null) {
      Checks.checkTrue(
          RESERVED_FIELDS.stream().noneMatch(extraInfo::containsKey),
          "extra info %s cannot contain any reserved keys %s",
          extraInfo.keySet(),
          RESERVED_FIELDS);
    }
    this.extraInfo = extraInfo;
  }

  /** extraInfo includes optional extra information. */
  @JsonAnyGetter
  public Map<String, Object> getExtraInfo() {
    return extraInfo;
  }

  /** Add fields to extraInfo. */
  @JsonAnySetter
  public void add(String name, Object value) {
    extraInfo.put(name, value);
  }
}
