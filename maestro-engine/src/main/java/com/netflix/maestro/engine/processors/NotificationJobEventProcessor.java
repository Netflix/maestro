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

import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.queue.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.jobevents.NotificationJobEvent;
import com.netflix.maestro.queue.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.queue.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.queue.processors.MaestroEventProcessor;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NotificationJobEventProcessor implements MaestroEventProcessor<NotificationJobEvent> {
  private final MaestroNotificationPublisher eventClient;
  private final String clusterName;

  @Override
  public Optional<MaestroJobEvent> process(NotificationJobEvent notificationJobEvent) {
    if (notificationJobEvent != null && notificationJobEvent.getEvent() != null) {
      MaestroJobEvent jobEvent = notificationJobEvent.getEvent();
      switch (jobEvent.getType()) {
        case STEP_INSTANCE_UPDATE:
          eventClient.send(((StepInstanceUpdateJobEvent) jobEvent).toMaestroEvent(clusterName));
          break;
        case WORKFLOW_INSTANCE_UPDATE:
          ((WorkflowInstanceUpdateJobEvent) jobEvent)
              .toMaestroEventStream(clusterName)
              .forEach(eventClient::send);
          break;
        case WORKFLOW_VERSION_UPDATE:
          eventClient.send(((WorkflowVersionUpdateJobEvent) jobEvent).toMaestroEvent(clusterName));
          break;
        case DELETE_WORKFLOW:
          eventClient.send(((DeleteWorkflowJobEvent) jobEvent).toMaestroEvent(clusterName));
          break;
        default:
          throw new MaestroInternalError(
              "Unsupported event type: %s , ignoring this event: %s", jobEvent.getType(), jobEvent);
      }
    }
    return Optional.empty();
  }
}
