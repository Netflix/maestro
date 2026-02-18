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

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;

/**
 * Implementation of {@link MaestroDataProvider} that delegates to existing engine DAOs. In OSS
 * Maestro, all components run in the same JVM, so we can access data directly via DAOs rather than
 * making HTTP calls like the internal MaestroClient.
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class DaoBackedMaestroDataProvider implements MaestroDataProvider {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroWorkflowInstanceDao workflowInstanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;

  public DaoBackedMaestroDataProvider(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowInstanceDao workflowInstanceDao,
      MaestroStepInstanceDao stepInstanceDao) {
    this.workflowDao = workflowDao;
    this.workflowInstanceDao = workflowInstanceDao;
    this.stepInstanceDao = stepInstanceDao;
  }

  @Override
  public WorkflowInstance getWorkflowInstance(String workflowId, long instanceId, long runId) {
    return workflowInstanceDao.getWorkflowInstanceRun(workflowId, instanceId, runId);
  }

  @Override
  public WorkflowDefinition getWorkflowDefinition(String workflowId, String version) {
    return workflowDao.getWorkflowDefinition(workflowId, version);
  }

  @Override
  public StepInstance getStepInstance(
      String workflowId, long instanceId, long runId, String stepId, long attemptId) {
    return stepInstanceDao.getStepInstance(
        workflowId, instanceId, runId, stepId, String.valueOf(attemptId));
  }
}
