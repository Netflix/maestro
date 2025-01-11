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
package com.netflix.maestro.engine.processors;

import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.db.InstanceRunUuid;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateThenRunInstanceJobEvent;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

/**
 * Processor to consume {@link TerminateThenRunInstanceJobEvent} and terminate maestro workflow
 * instances for all cases. It monitors until workflow instances are terminated eventually. Then it
 * runs a new workflow instance.
 */
@AllArgsConstructor
public class TerminateThenRunInstanceJobProcessor
    implements MaestroEventProcessor<TerminateThenRunInstanceJobEvent> {
  private final MaestroJobEventPublisher publisher;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final TerminateInstancesJobProcessor terminateInstancesJobProcessor;

  @Override
  public void process(Supplier<TerminateThenRunInstanceJobEvent> messageSupplier) {
    TerminateThenRunInstanceJobEvent jobEvent = messageSupplier.get();
    TerminateInstancesJobEvent terminationEvent =
        TerminateInstancesJobEvent.init(
            jobEvent.getWorkflowId(),
            jobEvent.getAction(),
            jobEvent.getUser(),
            jobEvent.getReason());
    terminationEvent.setInstanceRunUuids(jobEvent.getInstanceRunUuids());
    List<InstanceRunUuid> terminated =
        terminateInstancesJobProcessor.tryTerminateWorkflowInstances(terminationEvent);

    // then monitor the termination for workflows marked as terminated
    checkProgress(jobEvent.getWorkflowId(), terminated);

    // after that, run the workflow instance
    if (jobEvent.getRunAfter() != null) {
      RunWorkflowInstancesJobEvent runJobEvent =
          RunWorkflowInstancesJobEvent.init(jobEvent.getWorkflowId());
      runJobEvent.addOneRun(jobEvent.getRunAfter());
      publisher.publishOrThrow(
          runJobEvent, "Failed to send run job event after termination and will retry it.");
    }
  }

  private void checkProgress(String workflowId, List<InstanceRunUuid> instanceRunUuids) {
    instanceRunUuids.forEach(
        runUuid -> {
          WorkflowInstance.Status status =
              instanceDao.getWorkflowInstanceStatus(
                  workflowId, runUuid.getInstanceId(), runUuid.getRunId());
          if (status == null || !status.isTerminal()) {
            throw new MaestroRetryableError(
                "Workflow instance [%s][%s] is still terminating and will check it again",
                workflowId, runUuid);
          }
        });
  }
}
