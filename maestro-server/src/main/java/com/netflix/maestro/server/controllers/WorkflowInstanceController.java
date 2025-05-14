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

import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.WorkflowInstance;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Workflow instance related REST API. */
@Tag(name = "/api/v3/workflows", description = "Maestro Workflow Instance APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class WorkflowInstanceController {

  private final MaestroWorkflowInstanceDao workflowInstanceDao;

  @Autowired
  public WorkflowInstanceController(MaestroWorkflowInstanceDao workflowInstanceDao) {
    this.workflowInstanceDao = workflowInstanceDao;
  }

  @GetMapping(value = "/instances/action-map", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get workflow instance status to workflow instance action mapping")
  public Map<WorkflowInstance.Status, List<Actions.WorkflowInstanceAction>>
      getWorkflowInstanceActionMap() {
    return Actions.WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP;
  }

  @GetMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRun}",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get a specific workflow instance run (i.e. latest, or exact run id) ")
  public WorkflowInstance getWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRun") String workflowRun,
      @RequestParam(name = "enriched", defaultValue = "true") boolean enriched,
      @RequestParam(name = "aggregated", defaultValue = "false") boolean aggregated) {
    WorkflowInstance instance =
        workflowInstanceDao.getWorkflowInstance(
            workflowId, workflowInstanceId, workflowRun, aggregated);
    if (enriched) {
      instance.enrich();
    }

    if (!aggregated) {
      instance.setAggregatedInfo(null);
    }

    return instance;
  }

  @GetMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get a workflow instance view by overlaying data from all runs within it")
  public WorkflowInstance getWorkflowInstanceView(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @RequestParam(name = "enriched", defaultValue = "true") boolean enriched) {
    return getWorkflowInstance(
        workflowId, workflowInstanceId, Constants.LATEST_INSTANCE_RUN, enriched, true);
  }
}
