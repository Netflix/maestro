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

import com.netflix.maestro.flow.engine.FlowExecutor;
import com.netflix.maestro.flow.models.FlowDef;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Map;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
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

  private final FlowExecutor flowExecutor;

  @Autowired
  public FlowEngineController(FlowExecutor flowExecutor) {
    this.flowExecutor = flowExecutor;
  }

  public record StartFlowRequest(String flowId, FlowDef flowDef, Map<String, Object> flowInput) {}

  @PostMapping(
      value = "/{groupId}/flows/{flowReference}/start",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Start a flow in a group")
  public String startFlow(
      @PathVariable("groupId") long groupId,
      @Valid @NotNull @PathVariable("flowReference") String flowReference,
      @Valid @NotNull @RequestBody StartFlowRequest request) {
    return flowExecutor.startFlow(
        groupId, request.flowId(), flowReference, request.flowDef(), request.flowInput());
  }

  @PostMapping(
      value = "/{groupId}/flows/{flowReference}/tasks/{taskReference}/notify",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Wake up a specific flow task in a group")
  public Boolean wakeUp(
      @PathVariable("groupId") long groupId,
      @Valid @NotNull @PathVariable("flowReference") String flowReference,
      @Valid @NotNull @PathVariable("taskReference") String taskReference) {
    return flowExecutor.wakeUp(groupId, flowReference, taskReference);
  }

  @PostMapping(value = "/{groupId}/flows/notify", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Wake up all the tasks in a list of flows in a group")
  public Boolean wakeUp(
      @PathVariable("groupId") long groupId, @Valid @NotNull @RequestBody Set<String> refs) {
    return refs.stream().allMatch(ref -> flowExecutor.wakeUp(groupId, ref, null));
  }
}
