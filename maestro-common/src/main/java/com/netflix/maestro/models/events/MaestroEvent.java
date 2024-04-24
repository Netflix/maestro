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
package com.netflix.maestro.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Maestro event to carry the maestro status change information for downstream services.
 *
 * <p>Maestro event is not for change data capturing (CDC). It will not carry the change data
 * (delta). Instead, it only carries the necessary info to identify the maestro instance (either
 * workflow or step) status change. The current event size is capped at 16KB.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      name = "STEP_INSTANCE_STATUS_CHANGE_EVENT",
      value = StepInstanceStatusChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_INSTANCE_STATUS_CHANGE_EVENT",
      value = WorkflowInstanceStatusChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_DEFINITION_CHANGE_EVENT",
      value = WorkflowDefinitionChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_VERSION_CHANGE_EVENT",
      value = WorkflowVersionChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_PROPERTIES_CHANGE_EVENT",
      value = WorkflowPropertiesChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_ACTIVATION_CHANGE_EVENT",
      value = WorkflowActivationChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_DEACTIVATION_CHANGE_EVENT",
      value = WorkflowDeactivationChangeEvent.class),
  @JsonSubTypes.Type(
      name = "WORKFLOW_DELETION_CHANGE_EVENT",
      value = WorkflowDeletionChangeEvent.class)
})
public interface MaestroEvent {

  /** get the associated workflow id. */
  String getWorkflowId();

  /** get the associated cluster name. */
  String getClusterName();

  /** Get maestro event type. */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  Type getType();

  /** Get maestro event sync time, the moment that the event is synced internally. */
  long getSyncTime();

  /** Supported maestro event types. */
  enum Type {
    /** step instance status change event to external queue for downstream services. */
    STEP_INSTANCE_STATUS_CHANGE_EVENT,
    /** workflow instance status change event to external queue for downstream services. */
    WORKFLOW_INSTANCE_STATUS_CHANGE_EVENT,
    /** workflow version and properties change event to external queue for downstream services. */
    WORKFLOW_DEFINITION_CHANGE_EVENT,
    /** workflow version change event to external queue for downstream services. */
    WORKFLOW_VERSION_CHANGE_EVENT,
    /** workflow properties change event to external queue for downstream services. */
    WORKFLOW_PROPERTIES_CHANGE_EVENT,
    /** workflow activation change event to external queue for downstream services. */
    WORKFLOW_ACTIVATION_CHANGE_EVENT,
    /** workflow deactivation change event to external queue for downstream services. */
    WORKFLOW_DEACTIVATION_CHANGE_EVENT,
    /** workflow deletion change event to external queue for downstream services. */
    WORKFLOW_DELETION_CHANGE_EVENT
  }
}
