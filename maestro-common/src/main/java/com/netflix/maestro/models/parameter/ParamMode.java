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
import java.util.Locale;

/** Parameter mode. */
public enum ParamMode {
  /**
   * The parameter value cannot be changed by the user or the system, e.g. cannot modify it when
   * starting or restarting it. If a parameter is an expression or related to other parameters,
   * during restart, its evaluated result will not change and stay to be the same as the previous
   * run.
   */
  CONSTANT,
  /**
   * The parameter value cannot be changed by the user, e.g. cannot modify its definition when
   * starting or restarting it. But the parameter will be re-evaluated based on its definition. If a
   * parameter is an expression or related to other parameters, during restart, its evaluated result
   * might be changed if the re-evaluation will have a different result.
   */
  IMMUTABLE,
  /**
   * This is the default param mode for parameters. This mode allows parameter to be changed at any
   * time, including start, restart, as well as output parameters. During restart, if users update a
   * param definition, the parameter value will be re-evaluated based on the user input. Otherwise,
   * its evaluated result will not change and stay to be the same as the previous run.
   */
  MUTABLE,
  /**
   * The parameter value can be set on start, but cannot be changed by the user code at runtime.
   * During restart, its evaluated result will not change and stay to be the same as the previous
   * run.
   */
  MUTABLE_ON_START,
  /**
   * The parameter can be changed on start or restart, but not output parameter. During restart, if
   * users update a param definition, the parameter value will be re-evaluated based on the user
   * input. Otherwise, its evaluated result will not change and stay to be the same as the previous
   * run.
   */
  MUTABLE_ON_START_RESTART;

  /** ParamMode creator. */
  @JsonCreator
  public static ParamMode create(String mode) {
    return ParamMode.valueOf(mode.toUpperCase(Locale.US));
  }
}
