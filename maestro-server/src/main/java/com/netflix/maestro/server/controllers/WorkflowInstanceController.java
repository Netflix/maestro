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
import com.netflix.maestro.models.api.PaginationDirection;
import com.netflix.maestro.models.api.PaginationResult;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.server.utils.PaginationHelper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
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
  private static final int WORKFLOW_INSTANCE_MAX_BATCH_LIMIT = 200;
  private static final int WORKFLOW_INSTANCE_MIN_BATCH_LIMIT = 1;

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

  @GetMapping(value = "/{workflowId}/instances", consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Get workflow instances with pagination support",
      description =
          "Retrieves all the latest workflow instance runs for a given workflow ID with cursor-based pagination. "
              + "Use 'first' parameter for forward pagination or 'last' parameter for backward pagination, but not both. "
              + "The cursor parameter can be used to continue pagination from a specific workflow instance ID.")
  public PaginationResult<WorkflowInstance> getWorkflowInstances(
      @PathVariable("workflowId") @NotEmpty String workflowId,
      @Parameter(
              description =
                  "Number of results to return for forward pagination. Must be between 1 and 200 inclusive. "
                      + "Cannot be used together with 'last' parameter.")
          @RequestParam(name = "first", required = false)
          @Max(WORKFLOW_INSTANCE_MAX_BATCH_LIMIT)
          @Min(WORKFLOW_INSTANCE_MIN_BATCH_LIMIT)
          Long first,
      @Parameter(
              description =
                  "Number of results to return for backward pagination. Must be between 1 and 200 inclusive. "
                      + "Cannot be used together with 'first' parameter.")
          @RequestParam(name = "last", required = false)
          @Max(WORKFLOW_INSTANCE_MAX_BATCH_LIMIT)
          @Min(WORKFLOW_INSTANCE_MIN_BATCH_LIMIT)
          Long last,
      @Parameter(
              description =
                  "Cursor for pagination continuation. Should be a workflow instance ID. "
                      + "If not provided, pagination starts from the beginning (first) or end (last).")
          @RequestParam(name = "cursor", required = false)
          String cursor,
      @Parameter(
              description =
                  "Whether to return aggregated workflow instance status from all runs. "
                      + "When true, includes aggregated status information across all runs of each instance.")
          @RequestParam(name = "aggregated", defaultValue = "true")
          boolean aggregated) {
    return getWorkflowInstancesResult(workflowId, first, last, cursor, aggregated);
  }

  private PaginationResult<WorkflowInstance> getWorkflowInstancesResult(
      String workflowId, Long first, Long last, String cursor, boolean aggregated) {
    PaginationDirection direction = PaginationHelper.validateParamAndDeriveDirection(first, last);
    long limit = (last == null) ? first : last;

    // Early return for empty results
    long[] minMaxWorkflowInstanceIds = workflowInstanceDao.getMinMaxWorkflowInstanceIds(workflowId);
    if (minMaxWorkflowInstanceIds == null) {
      return PaginationHelper.buildEmptyPaginationResult();
    }

    long earliestWorkflowInstanceId = minMaxWorkflowInstanceIds[0];
    long latestWorkflowInstanceId = minMaxWorkflowInstanceIds[1];

    // Calculate pagination range
    PaginationHelper.PaginationRange paginationRange =
        PaginationHelper.getPaginationRange(
            cursor, direction, earliestWorkflowInstanceId, latestWorkflowInstanceId, limit);

    // Fetch workflow instances
    List<WorkflowInstance> workflowInstances =
        workflowInstanceDao.getWorkflowInstancesWithLatestRun(
            workflowId, paginationRange.start(), paginationRange.end(), aggregated);

    // Build pagination result with optimized range parser
    return PaginationHelper.buildPaginationResult(
        workflowInstances,
        latestWorkflowInstanceId,
        earliestWorkflowInstanceId,
        this::extractInstanceIdRange);
  }

  /** Extracts the high and low workflow instance IDs from a list of instances. */
  private long[] extractInstanceIdRange(List<WorkflowInstance> instances) {
    if (instances == null || instances.isEmpty()) {
      return new long[] {0L, 0L};
    }

    if (instances.size() == 1) {
      long instanceId = instances.getFirst().getWorkflowInstanceId();
      return new long[] {instanceId, instanceId};
    }

    // Find min and max by scanning the list
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (WorkflowInstance instance : instances) {
      long id = instance.getWorkflowInstanceId();
      min = Math.min(min, id);
      max = Math.max(max, id);
    }

    return new long[] {max, min}; // high, low
  }
}
