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

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowActionResponse;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.api.WorkflowStartRequest;
import com.netflix.maestro.models.api.WorkflowStartResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.timeline.TimelineActionEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.validations.JsonSizeConstraint;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.LinkedHashMap;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Workflow action related REST API. */
@Tag(name = "/api/v3/workflows", description = "Maestro Workflow Action APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class WorkflowActionController {
  private final WorkflowActionHandler actionHandler;
  private final User.UserBuilder callerBuilder;

  @Autowired
  public WorkflowActionController(
      WorkflowActionHandler actionHandler, User.UserBuilder callerBuilder) {
    this.actionHandler = actionHandler;
    this.callerBuilder = callerBuilder;
  }

  @PostMapping(
      value = "/{workflowId}/versions/{version}/actions/start",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Start a workflow instance for a given workflow version (i.e. active, latest, default, or an exact version id)")
  public WorkflowStartResponse startWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @PathVariable("version") String version,
      @Valid @NotNull @RequestBody WorkflowStartRequest request) {
    RunRequest runRequest = toRunRequest(request, callerBuilder.build());
    RunResponse runResponse = actionHandler.start(workflowId, version, runRequest);
    return runResponse.toWorkflowStartResponse();
  }

  private RunRequest toRunRequest(WorkflowStartRequest request, User caller) {
    request.getInitiator().setCaller(caller);
    return RunRequest.builder()
        .initiator(request.getInitiator())
        .requestTime(request.getRequestTime())
        .requestId(request.getRequestId())
        .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
        .runParams(ObjectHelper.valueOrDefault(request.getRunParams(), new LinkedHashMap<>()))
        .persistFailedRun(request.isPersistFailedRun())
        .build();
  }

  @PutMapping(value = "/actions/validate", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Validate the given workflow definition")
  public TimelineEvent validateWorkflow(
      @Valid
          @NotNull
          @RequestBody
          @JsonSizeConstraint(Constants.WORKFLOW_CREATE_REQUEST_DATA_SIZE_LIMIT)
          WorkflowCreateRequest request) {
    actionHandler.validate(request, callerBuilder.build());
    return TimelineActionEvent.builder()
        .action(Actions.WorkflowAction.VALIDATE)
        .author(callerBuilder.build())
        .message("the workflow definition is valid.")
        .build();
  }

  @PutMapping(value = "/{workflowId}/actions/deactivate", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Deactivate a workflow based on the given workflow id")
  public WorkflowActionResponse deactivateWorkflow(
      @Valid @NotNull @PathVariable("workflowId") String workflowId) {
    return actionHandler.deactivate(workflowId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/versions/{version}/actions/activate",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Activate a specific workflow version (i.e. active, latest, default, or exact version id)")
  public WorkflowActionResponse activateWorkflowVersion(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @Valid @NotNull @PathVariable("version") String version) {
    return actionHandler.activate(workflowId, version, callerBuilder.build());
  }
}
