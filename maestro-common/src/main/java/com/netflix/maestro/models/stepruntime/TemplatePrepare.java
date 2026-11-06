/*
 * Copyright 2026 Netflix, Inc.
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
import com.netflix.maestro.models.definition.Step;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * How a template step produces the step list it runs. The {@code type} selects a preparer
 * registered in the engine and is an open string so that deployments can add preparers without a
 * model change. Each built-in type uses one field: {@code static} stores the list in {@code steps},
 * {@code sel} evaluates {@code expression} to the expansion payload, and {@code step} runs {@code
 * step} as a generator whose {@code expansion} output param holds the payload.
 */
@JsonDeserialize(builder = TemplatePrepare.TemplatePrepareBuilder.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"type", "steps", "expression", "step"},
    alphabetic = true)
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
public class TemplatePrepare {
  /** Preparer type whose step list is stored in the registration. */
  public static final String STATIC = "static";

  /** Preparer type whose expansion payload is a SEL expression over the template inputs. */
  public static final String SEL = "sel";

  /** Preparer type whose expansion payload is produced by a registered generator step. */
  public static final String STEP = "step";

  @NotNull private final String type;

  @Valid private final List<Step> steps;

  private final String expression;

  @Valid private final Step step;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class TemplatePrepareBuilder {}
}
