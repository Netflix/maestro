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
package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDeletionDao;
import com.netflix.maestro.engine.utils.WorkflowEnrichmentHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.api.WorkflowCreateResponse;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.validations.JsonSizeConstraint;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Workflow related REST API. */
@io.swagger.v3.oas.annotations.tags.Tag(
    name = "/api/v3/workflows",
    description = "Maestro Workflow APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class WorkflowController {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroWorkflowDeletionDao workflowDeletionDao;
  private final User.UserBuilder callerBuilder;
  private final DryRunValidator dryRunValidator;
  private final WorkflowEnrichmentHelper workflowEnrichmentHelper;

  @Autowired
  public WorkflowController(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowDeletionDao workflowDeletionDao,
      User.UserBuilder callerBuilder,
      DryRunValidator dryRunValidator,
      WorkflowEnrichmentHelper workflowEnrichmentHelper) {
    this.workflowDao = workflowDao;
    this.workflowDeletionDao = workflowDeletionDao;
    this.callerBuilder = callerBuilder;
    this.dryRunValidator = dryRunValidator;
    this.workflowEnrichmentHelper = workflowEnrichmentHelper;
  }

  @PostMapping(value = "", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create or update a workflow definition")
  public WorkflowCreateResponse addWorkflow(
      @Valid
          @NotNull
          @RequestBody
          @JsonSizeConstraint(Constants.WORKFLOW_CREATE_REQUEST_DATA_SIZE_LIMIT)
          WorkflowCreateRequest request) {
    User caller = callerBuilder.build();
    dryRunValidator.validate(request.getWorkflow(), caller);
    WorkflowDefinition workflowDefinition = buildPartialWorkflowDefinition(request, caller);

    if (workflowDeletionDao.isDeletionInProgress(request.getWorkflow().getId())) {
      throw new MaestroResourceConflictException(
          "Cannot push a version for workflow [%s] while the system is still deleting the old data "
              + "for this workflow id. Please retry later after the deletion completes.",
          request.getWorkflow().getId());
    }

    WorkflowDefinition workflowDef =
        workflowDao.addWorkflowDefinition(workflowDefinition, request.getProperties());
    return WorkflowCreateResponse.builder().workflowDefinition(workflowDef).build();
  }

  /**
   * Build a workflow definition with the info from the request. It is incomplete and the additional
   * info will be filled during the workflow definition creation.
   */
  private WorkflowDefinition buildPartialWorkflowDefinition(
      WorkflowCreateRequest request, User author) {
    WorkflowDefinition workflowDef = new WorkflowDefinition();
    workflowDef.setWorkflow(request.getWorkflow());

    // build the metadata
    Metadata metadata = new Metadata();
    metadata.setWorkflowId(request.getWorkflow().getId());
    // workflow version is still unknown
    metadata.setCreateTime(System.currentTimeMillis());
    metadata.setVersionAuthor(author);
    metadata.setGitInfo(request.getGitInfo());
    metadata.setExtraInfo(request.getExtraInfo());

    workflowDef.setMetadata(metadata);
    // properties snapshot is still unknown
    workflowDef.setIsActive(request.getIsActive());
    if (workflowDef.getIsActive()) {
      workflowDef.setActivateTime(metadata.getCreateTime());
      workflowDef.setActivatedBy(metadata.getVersionAuthor());
    }
    return workflowDef;
  }

  @DeleteMapping(value = "/{workflowId}", consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Remove all the workflow data for the given workflow id, including definitions and instances")
  public TimelineEvent deleteWorkflow(
      @Valid @NotNull @PathVariable("workflowId") String workflowId) {
    return workflowDao.deleteWorkflow(workflowId, callerBuilder.build());
  }

  @Operation(
      summary =
          "Get the workflow definition for a specific workflow version "
              + "(i.e. active, latest, default, or exact version id)")
  @GetMapping(value = "/{workflowId}/versions/{version}", consumes = MediaType.ALL_VALUE)
  public WorkflowDefinition getWorkflowVersion(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @PathVariable("version") String version,
      @Parameter(description = "Enrich the workflow definition with the extra information.")
          @RequestParam(name = "enriched", required = false, defaultValue = "false")
          boolean enriched) {
    WorkflowDefinition workflowDefinition = workflowDao.getWorkflowDefinition(workflowId, version);
    if (enriched) {
      workflowEnrichmentHelper.enrichWorkflowDefinition(workflowDefinition);
    }
    return workflowDefinition;
  }
}
