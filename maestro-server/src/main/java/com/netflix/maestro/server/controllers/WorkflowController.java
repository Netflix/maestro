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
import com.netflix.maestro.engine.db.PropertiesUpdate;
import com.netflix.maestro.engine.db.PropertiesUpdate.Type;
import com.netflix.maestro.engine.utils.WorkflowEnrichmentHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.api.WorkflowCreateResponse;
import com.netflix.maestro.models.api.WorkflowOverviewResponse;
import com.netflix.maestro.models.api.WorkflowPropertiesUpdateRequest;
import com.netflix.maestro.models.definition.Metadata;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.WorkflowTimeline;
import com.netflix.maestro.validations.JsonSizeConstraint;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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
  private static final Set<Type> VALID_UPDATE_PROPERTY_TAGS_TYPES =
      Set.of(Type.ADD_WORKFLOW_TAG, Type.DELETE_WORKFLOW_TAG);

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

    updateOwnerIfNeeded(request.getProperties());
    validateProperties(
        request.getWorkflow().getId(), request.getProperties(), Type.ADD_WORKFLOW_DEFINITION);

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

  // put owner auth info into properties
  private void updateOwnerIfNeeded(Properties properties) {
    if (properties != null && properties.getOwner() != null) {
      User owner =
          User.builder()
              .name(properties.getOwner().getName())
              // inject the customized caller info here
              .extraInfo(properties.getOwner().getExtraInfo())
              .build();
      properties.setOwner(owner);
    }
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

  @PostMapping(value = "/{workflowId}/properties", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Create or update a workflow properties")
  public PropertiesSnapshot updateProperties(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @RequestBody WorkflowPropertiesUpdateRequest request) {
    Properties changes = request.getProperties();
    updateOwnerIfNeeded(changes);
    return updateProperties(workflowId, changes, new PropertiesUpdate(request));
  }

  private void validateProperties(
      String workflowId, Properties changes, PropertiesUpdate.Type updateType) {
    if (changes != null
        && changes.getTags() != null
        && !changes.getTags().getTags().isEmpty()
        && !VALID_UPDATE_PROPERTY_TAGS_TYPES.contains(updateType)) {
      throw new MaestroValidationException(
          "Cannot set workflow property tags (also known as dynamic tags) for workflow [%s]. Please use addWorkflowTag endpoint for that.",
          workflowId);
    }
  }

  @PostMapping(value = "/{workflowId}/properties/tag", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Add a tag to workflow properties")
  public PropertiesSnapshot addWorkflowTag(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @RequestBody Tag tagToBeAdded) {
    updateTagAttributes(tagToBeAdded);
    Properties props = new Properties();
    props.setTags(new TagList(Collections.singletonList(tagToBeAdded)));
    return updateProperties(
        workflowId, props, new PropertiesUpdate(PropertiesUpdate.Type.ADD_WORKFLOW_TAG));
  }

  @DeleteMapping(
      value = "/{workflowId}/properties/tag/{workflowTagName}",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Delete a tag from workflow properties")
  public PropertiesSnapshot deleteWorkflowTag(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @PathVariable("workflowTagName") String workflowTagName) {
    Tag tagToBeRemoved = Tag.create(workflowTagName);
    Properties props = new Properties();
    props.setTags(new TagList(Collections.singletonList(tagToBeRemoved)));
    return updateProperties(
        workflowId, props, new PropertiesUpdate(PropertiesUpdate.Type.DELETE_WORKFLOW_TAG));
  }

  private PropertiesSnapshot updateProperties(
      String workflowId, Properties changes, PropertiesUpdate update) {
    try {
      return workflowDao.updateWorkflowProperties(
          workflowId, callerBuilder.build(), changes, update);
    } catch (RuntimeException e) { // special handling here
      if (e.getMessage() != null
          && e.getMessage()
              .startsWith("BACKEND_ERROR - ERROR: failed to satisfy CHECK constraint")) {
        throw new MaestroBadRequestException(
            e.getCause(),
            "ERROR: please check if there exists this workflow: " + workflowId,
            e.getMessage());
      } else {
        throw e;
      }
    }
  }

  private void updateTagAttributes(Tag tagToBeAdded) {
    User caller = callerBuilder.build();
    Long createdAt = System.currentTimeMillis();
    Map<String, Object> attributes = tagToBeAdded.getAttributes();
    attributes.put("creator", caller);
    attributes.put("created_at_milli", createdAt);
    // additional attributes for platform tags go here.
    tagToBeAdded.setAttributes(attributes);
    tagToBeAdded.setNamespace(Tag.Namespace.PLATFORM);
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

  @Operation(
      summary =
          "Get the workflow properties snapshot for a snapshot id (i.e. latest or an exact snapshot id)")
  @GetMapping(
      value = "/{workflowId}/properties-snapshot/{snapshotId}",
      consumes = MediaType.ALL_VALUE)
  public PropertiesSnapshot getWorkflowPropertiesSnapshot(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @PathVariable("snapshotId") String snapshotId) {
    return workflowDao.getWorkflowPropertiesSnapshot(workflowId, snapshotId);
  }

  @Operation(
      summary =
          "Get workflow overview including version info and instance status stats for a specific workflow")
  @GetMapping(value = "/{workflowId}/overview", consumes = MediaType.ALL_VALUE)
  public WorkflowOverviewResponse getWorkflowOverview(
      @Valid @NotNull @PathVariable("workflowId") String workflowId) {
    return workflowDao.getWorkflowOverview(workflowId);
  }

  @Operation(
      summary =
          "Get most recent (max 300) workflow timeline info (workflow level change history) for a specific workflow. ")
  @GetMapping(value = "/{workflowId}/timeline", consumes = MediaType.ALL_VALUE)
  public WorkflowTimeline getWorkflowTimeline(
      @Valid @NotNull @PathVariable("workflowId") String workflowId) {
    return workflowDao.getWorkflowTimeline(workflowId);
  }
}
