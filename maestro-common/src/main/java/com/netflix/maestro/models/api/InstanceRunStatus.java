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

/**
 * Instance run status in the response of either WorkflowStartRequest or
 * WorkflowInstanceRestartRequest.
 */
public enum InstanceRunStatus {
  /** workflow instance is created. */
  CREATED,
  /** workflow instance with the same uuid already exists, won't create a new one. */
  DUPLICATED,
  /** workflow instance is stopped due to run strategy, i.e. FIRST_ONLY or LAST_ONLY. */
  STOPPED,
  /** there is an internal error. */
  INTERNAL_ERROR;
}
