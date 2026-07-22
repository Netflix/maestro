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
package com.netflix.maestro.queue.jobevents;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.queue.models.MessageDto;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

/** Those are maestro internal job events, which won't be emitted externally. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "START_WORKFLOW", value = StartWorkflowJobEvent.class),
  @JsonSubTypes.Type(name = "TERMINATE_THEN_RUN", value = TerminateThenRunJobEvent.class),
  @JsonSubTypes.Type(name = "INSTANCE_ACTION", value = InstanceActionJobEvent.class),
  @JsonSubTypes.Type(name = "STEP_INSTANCE_UPDATE", value = StepInstanceUpdateJobEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_INSTANCE_UPDATE",
      value = WorkflowInstanceUpdateJobEvent.class),
  @JsonSubTypes.Type(name = "WORKFLOW_VERSION_UPDATE", value = WorkflowVersionUpdateJobEvent.class),
  @JsonSubTypes.Type(name = "NOTIFICATION", value = NotificationJobEvent.class),
  @JsonSubTypes.Type(name = "SIGNAL_INSTANCE", value = SignalInstanceJobEvent.class),
  @JsonSubTypes.Type(name = "SIGNAL_TRIGGER_MATCH", value = SignalTriggerMatchJobEvent.class),
  @JsonSubTypes.Type(
      name = "SIGNAL_TRIGGER_EXECUTION",
      value = SignalTriggerExecutionJobEvent.class),
  @JsonSubTypes.Type(name = "DELETE_WORKFLOW", value = DeleteWorkflowJobEvent.class),
})
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface MaestroJobEvent {
  /** Get a maestro event type. */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  Type getType();

  /** Supported maestro job event types for internal job queue. */
  enum Type {
    /** Internal job event to run strategy to start workflow instance(s) for a workflow id. */
    START_WORKFLOW(1),
    /**
     * Internal job event to terminate a batch of workflow instance(s) and then run a batch of
     * workflow instance(s).
     */
    TERMINATE_THEN_RUN(2),
    /** step/workflow instance job event to publish a request for step instance wake up. */
    INSTANCE_ACTION(2),
    /** step instance job event to publish step instance status changes. */
    STEP_INSTANCE_UPDATE(3),
    /** workflow instance job event to publish workflow instance status changes. */
    WORKFLOW_INSTANCE_UPDATE(3),
    /** workflow definition job event to publish definition changes. */
    WORKFLOW_VERSION_UPDATE(3),
    /** Internal job event to send notifications. */
    NOTIFICATION(4),
    /** Internal job event to process a signal instance. */
    SIGNAL_INSTANCE(4),
    /** Internal job event to process a signal trigger match. */
    SIGNAL_TRIGGER_MATCH(4),
    /** Internal job event to process a signal trigger execution. */
    SIGNAL_TRIGGER_EXECUTION(4),
    /** Internal job event to DELETE all related workflow data for a workflow id. */
    DELETE_WORKFLOW(5);

    @Getter private final int queueId;

    Type(int queueId) {
      this.queueId = queueId;
    }
  }

  /** return a random uuid as a message key. todo make it more readable than uuid. */
  @JsonIgnore
  default String deriveMessageKey() {
    return UUID.randomUUID().toString();
  }

  @SuppressWarnings({"PMD.LooseCoupling", "PMD.AvoidInstantiatingObjectsInLoops"})
  static EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> createQueuesForJobEvents() {
    EnumMap<MaestroJobEvent.Type, BlockingQueue<MessageDto>> eventQueues =
        new EnumMap<>(MaestroJobEvent.Type.class);
    Map<Integer, BlockingQueue<MessageDto>> queues = new HashMap<>();
    for (var entry : MaestroJobEvent.Type.values()) {
      if (queues.containsKey(entry.getQueueId())) {
        // share the same queue for job type with the same queue id
        eventQueues.put(entry, queues.get(entry.getQueueId()));
      } else {
        BlockingQueue<MessageDto> messageQueue = new LinkedBlockingQueue<>();
        queues.put(entry.getQueueId(), messageQueue);
        eventQueues.put(entry, messageQueue);
      }
    }
    return eventQueues;
  }
}
