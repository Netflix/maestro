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
package com.netflix.maestro.engine.eval;

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.models.definition.StepType;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

/**
 * An internal wrapper class. It is used to wrap the selected metadata from a step instance that can
 * be returned by SEL MaestroParamExtension calls.
 */
@Getter
@Builder(access = AccessLevel.PRIVATE)
class StepInstanceAttributes {
  private final String stepId;
  private final StepType type;
  private final String subType;
  private final long stepInstanceId;
  private final long stepAttemptId;
  private final String stepInstanceUuid;

  static StepInstanceAttributes from(StepRuntimeSummary summary) {
    return StepInstanceAttributes.builder()
        .type(summary.getType())
        .subType(summary.getSubType())
        .stepId(summary.getStepId())
        .stepInstanceId(summary.getStepInstanceId())
        .stepAttemptId(summary.getStepAttemptId())
        .stepInstanceUuid(summary.getStepInstanceUuid())
        .build();
  }
}
