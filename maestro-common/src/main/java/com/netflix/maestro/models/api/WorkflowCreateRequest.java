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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.GitInfo;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.validations.PropertiesConstraint;
import com.netflix.maestro.validations.WorkflowConstraint;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.Valid;
import lombok.Data;

/** Request to create a workflow definition. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"properties", "workflow", "is_active", "git_info"},
    alphabetic = true)
@Data
public class WorkflowCreateRequest {
  @Valid @PropertiesConstraint private Properties properties;
  @Valid @WorkflowConstraint private Workflow workflow;

  private Boolean isActive = Defaults.DEFAULT_WORKFLOW_ACTIVE_FLAG;
  private GitInfo gitInfo;
  private Map<String, Object> extraInfo = new LinkedHashMap<>();

  /** Get fields from extraInfo. */
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
