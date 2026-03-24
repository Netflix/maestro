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
package com.netflix.maestro.extensions.dao.models;

import com.netflix.maestro.models.instance.StepRuntimeState;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class ForeachFlattenedModel {
  @Valid @NotNull private final ForeachFlattenedInstance instance;

  @Min(1)
  private final long runIdValidityEnd;

  private final long stepStatusEncoded;

  private final long statusPriority; // 0 means unknown

  @Valid @NotNull private final StepRuntimeState stepRuntimeState;

  @NotNull private final String stepAttemptSeq;

  @NotNull private final Map<String, Object> loopParameters;
}
