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
package com.netflix.maestro.extensions.provider;

import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;

/**
 * Interface for providing Maestro data to the extensions module. This replaces the HTTP-based
 * MaestroClient used in internal Maestro, since in OSS everything runs in the same JVM and we can
 * access DAOs directly.
 */
public interface MaestroDataProvider {
  /**
   * Get a workflow instance for a specific run.
   *
   * @param workflowId id of the workflow
   * @param instanceId instance id
   * @param runId run id
   * @return the workflow instance
   */
  WorkflowInstance getWorkflowInstance(String workflowId, long instanceId, long runId);

  /**
   * Get a workflow definition for a specific version.
   *
   * @param workflowId id of the workflow
   * @param version version string
   * @return the workflow definition
   */
  WorkflowDefinition getWorkflowDefinition(String workflowId, String version);

  /**
   * Get a step instance.
   *
   * @param workflowId id of the workflow
   * @param instanceId instance id
   * @param runId run id
   * @param stepId id of the step
   * @param attemptId attempt id
   * @return the step instance
   */
  StepInstance getStepInstance(
      String workflowId, long instanceId, long runId, String stepId, long attemptId);
}
