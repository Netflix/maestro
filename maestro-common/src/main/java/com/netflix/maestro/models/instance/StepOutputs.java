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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.timeline.TimelineLogEvent;

/** Summarizes all the step outputs. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({@JsonSubTypes.Type(name = "SIGNAL", value = SignalStepOutputs.class)})
public interface StepOutputs {

  StepOutputsDefinition.StepOutputType getType();

  TimelineLogEvent getInfo();

  default SignalStepOutputs asSignalStepOutputs() {
    throw new MaestroInternalError(
        "StepOutputs is a [%s] type and cannot be used as SignalStepOutput", getType());
  }
}
