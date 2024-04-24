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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

/**
 * workflow instance run policy, which is used by both workflow start, workflow restart, and step
 * restart.
 */
@Getter
public enum RunPolicy {
  /** Start a new workflow instance run with run_id=1 from the beginning. */
  START_FRESH_NEW_RUN(true),
  /**
   * Start a new workflow instance run with run_id=1 with customized start, end, and skip step ids.
   * Not implemented and leave it for the future.
   */
  START_CUSTOMIZED_RUN(true),
  /** Restart a workflow instance run by creating a new run beginning with the incomplete steps. */
  RESTART_FROM_INCOMPLETE(false),
  /** Restart a workflow instance run from the beginning by creating a new run. */
  RESTART_FROM_BEGINNING(false),
  /** Restart a workflow instance run from a specific step by creating a new run. */
  RESTART_FROM_SPECIFIC(false),
  /**
   * Restart a workflow instance run with customized start, end, and skip step ids. Not implemented
   * and leave it for the future.
   */
  RESTART_CUSTOMIZED_RUN(false);

  @JsonIgnore private final boolean freshRun;

  RunPolicy(boolean freshRun) {
    this.freshRun = freshRun;
  }
}
