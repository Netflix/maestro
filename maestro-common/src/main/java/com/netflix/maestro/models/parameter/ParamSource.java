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
package com.netflix.maestro.models.parameter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Locale;
import lombok.Getter;

/** Parameter mode. */
@Getter
public enum ParamSource {
  /** System Defined. */
  SYSTEM(true),

  /** Set by Template Schema source. */
  TEMPLATE_SCHEMA(true),

  /** Injected by Maestro dynamically. */
  SYSTEM_INJECTED(true),

  /** System default value. */
  SYSTEM_DEFAULT(true),

  /** Set by Definition by user. */
  DEFINITION(false),

  /** Set by signal trigger. */
  SIGNAL(false),

  /** Set by time trigger. */
  TIME_TRIGGER(false),

  /** Set by output parameter. */
  OUTPUT_PARAMETER(false),

  /** Set by user launch. */
  LAUNCH(false),

  /** Set by user restart. */
  RESTART(false),

  /** Set by subworkflow parent. */
  SUBWORKFLOW(false),

  /** Set by template parent. */
  TEMPLATE(false),

  /** Set by foreach launch. */
  FOREACH(false),

  /** Set by while launch. */
  WHILE(false),

  /** Indicates the parameter is set by the workflow level parameter with the same name. */
  WORKFLOW_PARAMETER(false);

  /** Whether this is a system controlled stage vs user input. */
  @JsonIgnore private final boolean isSystemStage; // if it is terminal

  /** Constructor. */
  ParamSource(boolean isSystemStage) {
    this.isSystemStage = isSystemStage;
  }

  /** ParamMode creator. */
  @JsonCreator
  public static ParamSource create(String mode) {
    return ParamSource.valueOf(mode.toUpperCase(Locale.US));
  }
}
