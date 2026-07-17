/*
 * Copyright 2025 Netflix, Inc.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.MessagePayload;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.utils.IdHelper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Flow engine related REST API.
 *
 * @author jun-he
 */
@Tag(name = "/api/v3/groups", description = "Maestro Flow Engine APIs")
@RestController
@RequestMapping(
    value = "/api/v3/groups",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class FlowEngineController {
  private static final String GROUP_ID = "groupId";
  private static final String CODE = "code";

  private final FlowOperation flowOperation;
  private final ObjectMapper objectMapper;

  @Autowired
  public FlowEngineController(
      FlowOperation flowOperation,
      @Qualifier(Constants.MAESTRO_QUALIFIER) ObjectMapper objectMapper) {
    this.flowOperation = flowOperation;
    this.objectMapper = objectMapper;
  }

  public record StartFlowRequest(String flowId, FlowDef flowDef, Map<String, Object> flowInput) {}

  @PostMapping(
      value = "/{groupId}/flows/{flowReference}/start",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Start a flow in a group")
  public String startFlow(
      @PathVariable(GROUP_ID) long groupId,
      @Valid @NotNull @PathVariable("flowReference") String flowReference,
      @Valid @NotNull @RequestBody StartFlowRequest request) {
    // The request body was JSON-deserialized into a Map<String, Object>, so the workflow summary
    // value is an untyped LinkedHashMap. Re-materialize it once here into a WorkflowSummary so the
    // flow runs with the typed object and downstream reads cast instead of converting per poll.
    Map<String, Object> flowInput = request.flowInput();
    Object summary = flowInput.get(Constants.WORKFLOW_SUMMARY_FIELD);
    if (summary != null && !(summary instanceof WorkflowSummary)) {
      flowInput.put(
          Constants.WORKFLOW_SUMMARY_FIELD,
          objectMapper.convertValue(summary, WorkflowSummary.class));
    }
    return flowOperation.startFlow(
        groupId, request.flowId(), flowReference, request.flowDef(), flowInput);
  }

  @PostMapping(
      value = "/{groupId}/flows/{flowReference}/tasks/{taskReference}/notify/{code}",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Wake up a specific flow task in a group")
  public Boolean wakeUp(
      @PathVariable(GROUP_ID) long groupId,
      @Valid @NotNull @PathVariable("flowReference") String flowReference,
      @Valid @NotNull @PathVariable("taskReference") String taskReference,
      @PathVariable(CODE) int code) {
    return flowOperation.wakeUp(groupId, flowReference, taskReference, code);
  }

  @PostMapping(
      value = "/{groupId}/flows/notify/{code}",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Wake up all the tasks in a list of flows in a group")
  public Boolean wakeUp(
      @PathVariable(GROUP_ID) long groupId,
      @PathVariable(CODE) int code,
      @Valid @NotNull @RequestBody Set<String> refs) {
    return flowOperation.wakeUp(groupId, refs, code);
  }

  @PostMapping(
      value = "/{groupId}/flows/{flowReference}/tasks/{taskReference}/message/{code}",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Send a message payload to a specific flow task in a group")
  public Boolean wakeUp(
      @PathVariable(GROUP_ID) long groupId,
      @Valid @NotNull @PathVariable("flowReference") String flowReference,
      @Valid @NotNull @PathVariable("taskReference") String taskReference,
      @PathVariable(CODE) int code,
      @Valid @NotNull @RequestBody MessagePayload payload) {
    return flowOperation.wakeUp(groupId, flowReference, taskReference, code, payload);
  }

  @PostMapping(
      value =
          "/workflows/{workflowId}/instances/{instanceId}/runs/{runId}/groups/{groupInfo}/steps/{stepId}/message/{code}",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Send a message payload to a step by workflow reference")
  public Boolean wakeUpWorkflowStep(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("instanceId") long instanceId,
      @PathVariable("runId") long runId,
      @PathVariable("groupInfo") long groupInfo,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @PathVariable(CODE) int code,
      @Valid @NotNull @RequestBody MessagePayload payload) {
    String flowReference = IdHelper.deriveFlowRef(workflowId, instanceId, runId);
    long groupId = IdHelper.deriveGroupId(flowReference, groupInfo);
    return flowOperation.wakeUp(groupId, flowReference, stepId, code, payload);
  }
}
