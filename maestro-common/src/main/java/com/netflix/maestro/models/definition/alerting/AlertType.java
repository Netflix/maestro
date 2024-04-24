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
package com.netflix.maestro.models.definition.alerting;

import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.models.definition.Tct;

/** Type of alerts that can be configured in {@link Alerting}. */
public enum AlertType {
  /** When a step fails. */
  STEP_FAILURE,
  /** When a workflow's {@link Tct} is violated. */
  TCT_VIOLATION,
  /** When a workflow or step is running longer than usual. */
  LONG_RUNNING,
  /** When a workflow has a dependency that is coming late. */
  DEPENDENCY_LATE,
  /** When a step in a workflow hits a breakpoint. */
  BREAKPOINT_HIT,
  /** When a workflow's definition changes. */
  DEFINITION_CHANGE,
  /** When a workflow or step successfully completes. */
  RUN_SUCCESS,
  /** When a workflow has not succeeded within a time range. */
  SUCCESS_WITHIN,
}
