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
package com.netflix.maestro.engine.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.trigger.TriggerUuids;
import com.netflix.maestro.utils.Checks;
import javax.validation.Valid;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;

/**
 * This is maestro_workflow data model.
 *
 * <p>It includes a snapshot of the current active workflow definition. In case no active version,
 * it is the last active or pushed one. It also includes the aggregated properties.
 *
 * <p>For all workflow definition related ops, will load the info from this table first.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "internal_id",
      "active_version_id",
      "activate_time",
      "activated_by",
      "modify_time",
      "properties_snapshot",
      "latest_version_id",
      "metadata",
      "definition",
      "triggerUuids"
    },
    alphabetic = true)
@Data
@Builder
public final class MaestroWorkflow {
  private final String workflowId;
  private final Long internalId;
  private final Long activeVersionId;
  private final Long activateTime;
  private final User activatedBy;
  private final Long modifyTime;

  private final PropertiesSnapshot propertiesSnapshot;
  private final Long latestVersionId;
  @Setter private Metadata metadata;
  @Setter private Workflow definition;
  @Setter private TriggerUuids triggerUuids;

  private MaestroWorkflow(
      String workflowId,
      Long internalId,
      Long activeVersionId,
      Long activateTime,
      User activatedBy,
      Long modifyTime,
      PropertiesSnapshot propertiesSnapshot,
      Long latestVersionId,
      Metadata metadata,
      Workflow definition,
      TriggerUuids triggerUuids) {
    this.workflowId = workflowId;
    this.internalId = internalId;
    this.activeVersionId = activeVersionId;
    this.activateTime = activateTime;
    this.activatedBy = activatedBy;
    this.modifyTime = modifyTime;
    this.propertiesSnapshot = propertiesSnapshot;
    this.latestVersionId = latestVersionId;
    this.metadata = metadata;
    this.definition = definition;
    this.triggerUuids = triggerUuids;
    validate(this);
  }

  private static void validate(MaestroWorkflow mw) {
    Checks.notNull(mw.workflowId, "workflowId cannot be null for", mw);
    Checks.notNull(mw.internalId, "workflow unique internalId cannot be null for", mw);
    Checks.notNull(mw.activeVersionId, "Active version cannot be null for", mw);
    Checks.notNull(mw.propertiesSnapshot, "properties snapshot cannot be null for", mw);
    Checks.notNull(mw.propertiesSnapshot.getCreateTime(), "create time cannot be null for", mw);
    Checks.notNull(
        mw.propertiesSnapshot.getAuthor(), "properties snapshot author cannot be null for", mw);
    Checks.notNull(mw.propertiesSnapshot.getOwner(), "owner cannot be null for", mw);
    Checks.checkTrue(
        mw.activeVersionId == Constants.INACTIVE_VERSION_ID
            || (mw.activateTime != null && mw.activatedBy != null),
        "Workflow activated info fields are invalid for",
        mw);
    Checks.notNull(mw.latestVersionId, "latest version id cannot be null for", mw);
    Checks.notNull(mw.modifyTime, "modify time cannot be null for", mw);
  }

  /**
   * create a WorkflowDefinition using MaestroWorkflow.
   *
   * @return transformed workflow definition
   */
  public @Valid WorkflowDefinition toDefinition() {
    WorkflowDefinition workflowDef = new WorkflowDefinition();
    workflowDef.setPropertiesSnapshot(this.propertiesSnapshot);
    workflowDef.setMetadata(this.metadata);
    workflowDef.setWorkflow(this.definition);
    workflowDef.setTriggerUuids(this.triggerUuids);

    // update the metadata
    workflowDef.setIsActive(
        activeVersionId.equals(workflowDef.getMetadata().getWorkflowVersionId()));
    if (workflowDef.getIsActive()) {
      workflowDef.setActivateTime(this.activateTime);
      workflowDef.setActivatedBy(this.activatedBy);
    }

    Long defaultVersionId =
        this.activeVersionId.equals(Constants.INACTIVE_VERSION_ID)
            ? this.latestVersionId
            : this.activeVersionId;

    workflowDef.setIsLatest(
        this.latestVersionId.equals(workflowDef.getMetadata().getWorkflowVersionId()));
    workflowDef.setIsDefault(
        defaultVersionId.equals(workflowDef.getMetadata().getWorkflowVersionId()));
    workflowDef.setModifyTime(this.modifyTime);
    workflowDef.setInternalId(this.internalId);

    return workflowDef;
  }
}
