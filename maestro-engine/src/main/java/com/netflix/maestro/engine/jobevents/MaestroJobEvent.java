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
package com.netflix.maestro.engine.jobevents;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.UUID;

/** Those are maestro internal job events, which won't be emitted externally. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      name = "STEP_INSTANCE_UPDATE_JOB_EVENT",
      value = StepInstanceUpdateJobEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_INSTANCE_UPDATE_JOB_EVENT",
      value = WorkflowInstanceUpdateJobEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_VERSION_UPDATE_JOB_EVENT",
      value = WorkflowVersionUpdateJobEvent.class),
  @JsonSubTypes.Type(name = "START_WORKFLOW_JOB_EVENT", value = StartWorkflowJobEvent.class),
  @JsonSubTypes.Type(
      name = "RUN_WORKFLOW_INSTANCES_JOB_EVENT",
      value = RunWorkflowInstancesJobEvent.class),
  @JsonSubTypes.Type(
      name = "TERMINATE_INSTANCES_JOB_EVENT",
      value = TerminateInstancesJobEvent.class),
  @JsonSubTypes.Type(
      name = "TERMINATE_THEN_RUN_JOB_EVENT",
      value = TerminateThenRunInstanceJobEvent.class),
  @JsonSubTypes.Type(name = "DELETE_WORKFLOW_JOB_EVENT", value = DeleteWorkflowJobEvent.class),
  @JsonSubTypes.Type(
      name = "STEP_INSTANCE_WAKE_UP_JOB_EVENT",
      value = StepInstanceWakeUpEvent.class),
})
public interface MaestroJobEvent {
  /** Get maestro event type. */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  Type getType();

  /** Supported maestro job event types for internal job queue. */
  enum Type {
    /** step instance job event to publish step instance status changes. */
    STEP_INSTANCE_UPDATE_JOB_EVENT,
    /** step instance job event to publish a request for step instance wake up. */
    STEP_INSTANCE_WAKE_UP_JOB_EVENT,
    /** workflow instance job event to publish workflow instance status changes. */
    WORKFLOW_INSTANCE_UPDATE_JOB_EVENT,
    /** workflow definition job event to publish definition changes. */
    WORKFLOW_VERSION_UPDATE_JOB_EVENT,
    /** Internal job event to run strategy to start workflow instance(s) for a workflow id. */
    START_WORKFLOW_JOB_EVENT,
    /** Internal job event to run a batch of workflow instance(s). */
    RUN_WORKFLOW_INSTANCES_JOB_EVENT,

    /** Internal job event to terminate a batch of workflow instance(s). */
    TERMINATE_INSTANCES_JOB_EVENT,
    /**
     * Internal job event to terminate a batch of workflow instance(s) and then start a workflow
     * instance.
     */
    TERMINATE_THEN_RUN_JOB_EVENT,

    /** Internal job event to DELETE all related workflow data for a workflow id. */
    DELETE_WORKFLOW_JOB_EVENT;
  }

  /** return a random uuid as message key. */
  @JsonIgnore
  default String getMessageKey() {
    return UUID.randomUUID().toString();
  }
}
