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
package com.netflix.maestro.models.initiator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.parameter.ParamSource;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Subworkflow initiator to store its parent workflow and the subworkflow step info at runtime. */
@EqualsAndHashCode(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"ancestors", "depth", "type"},
    alphabetic = true)
@ToString(callSuper = true)
public final class SubworkflowInitiator extends UpstreamInitiator {
  @Override
  public Type getType() {
    return Type.SUBWORKFLOW;
  }

  @Override
  public ParamSource getParameterSource() {
    return ParamSource.SUBWORKFLOW;
  }
}
