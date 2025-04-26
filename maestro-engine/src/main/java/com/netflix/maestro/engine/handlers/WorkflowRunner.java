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
package com.netflix.maestro.engine.handlers;

import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Workflow runner to run a maestro workflow in the internal flow engine. */
@AllArgsConstructor
@Slf4j
public class WorkflowRunner {
  private final MaestroFlowDao flowDao;
  private final FlowOperation flowOperation;
  private final WorkflowTranslator translator;
  private final WorkflowHelper workflowHelper;

  /**
   * Launch a maestro workflow instance in the internal flow engine.
   *
   * @param instance Maestro workflow instance.
   * @param uuid UUID of the workflow instance.
   */
  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  public void run(WorkflowInstance instance, String uuid) {
    try {
      if (instance.getStatus() == WorkflowInstance.Status.CREATED) {
        if (uuid.equals(instance.getWorkflowUuid())) {
          if (!flowDao.existFlowWithSameKeys(
              IdHelper.deriveGroupId(instance), instance.getWorkflowUuid())) {
            String executionId = runWorkflowInstance(instance);
            LOG.info(
                "Run a workflow instance {} with an internal execution_id [{}]",
                instance.getIdentity(),
                executionId);
          } else {
            LOG.warn(
                "Workflow instance [{}][{}] has already been executed by flow engine. Skip it for dedup",
                instance.getIdentity(),
                uuid);
          }
        } else {
          LOG.warn(
              "Workflow instance: [{}][{}] in job event does not match DB row uuid [{}] and skip it",
              instance.getIdentity(),
              uuid,
              instance.getWorkflowUuid());
        }
      } else {
        LOG.info(
            "workflow instance: [{}][{}] is not in created (i.e. queued) state and skip it",
            instance.getIdentity(),
            uuid);
      }
    } catch (MaestroNotFoundException e) {
      // this is the case that DB is rollback but msg is sent.
      LOG.warn(
          "Not found workflow instance: [{}][{}] and skip it in workflow runner",
          instance.getIdentity(),
          uuid);
    } catch (NullPointerException e) {
      // not retryable
      LOG.error(
          "Cannot retry if there is a NullPointerException for workflow [{}]",
          instance.getIdentity());
      throw new MaestroInternalError(e, "Something is null");
    } catch (RuntimeException e) {
      LOG.warn("Retry it as getting a runtime error", e);
      throw new MaestroRetryableError(e, "Failed to run a workflow and will retry to run it.");
    }
  }

  private String runWorkflowInstance(WorkflowInstance instance) {
    if (instance.isFreshRun()) {
      return start(instance);
    } else {
      return restart(instance);
    }
  }

  /**
   * Run a maestro workflow in maestro flow engine.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the internal flow instance
   */
  String start(WorkflowInstance instance) {
    return flowOperation.startFlow(
        IdHelper.deriveGroupId(instance),
        instance.getWorkflowUuid(),
        IdHelper.deriveFlowRef(instance.getWorkflowId(), instance.getWorkflowInstanceId()),
        translator.translate(instance),
        Collections.singletonMap(
            Constants.WORKFLOW_SUMMARY_FIELD,
            workflowHelper.createWorkflowSummaryFromInstance(instance)));
  }

  /**
   * Restart a maestro workflow instance in maestro flow engine as a new run.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the internal flow instance
   */
  String restart(WorkflowInstance instance) {
    return start(instance);
  }
}
