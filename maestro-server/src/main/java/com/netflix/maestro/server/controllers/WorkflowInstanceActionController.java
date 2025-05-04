package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.StepInstanceActionHandler;
import com.netflix.maestro.engine.handlers.WorkflowInstanceActionHandler;
import com.netflix.maestro.models.api.WorkflowInstanceActionResponse;
import com.netflix.maestro.models.api.WorkflowInstanceRestartRequest;
import com.netflix.maestro.models.api.WorkflowInstanceRestartResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.utils.ObjectHelper;
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

/**
 * Workflow instance action related REST API.
 *
 * <p>All the actions for a workflow instance are applied to the latest run_id as all other runs
 * have already een in one of immutable terminal states.
 *
 * @author jun-he
 */
@Tag(name = "/api/v3/workflows", description = "Maestro Workflow Instance Action APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class WorkflowInstanceActionController {

  private final WorkflowInstanceActionHandler actionHandler;
  private final StepInstanceActionHandler stepActionHandler;
  private final User.UserBuilder callerBuilder;

  @Autowired
  public WorkflowInstanceActionController(
      WorkflowInstanceActionHandler actionHandler,
      StepInstanceActionHandler stepActionHandler,
      User.UserBuilder callerBuilder) {
    this.actionHandler = actionHandler;
    this.stepActionHandler = stepActionHandler;
    this.callerBuilder = callerBuilder;
  }

  @PostMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/actions/restart",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Restart a given workflow instance by creating a new run")
  public WorkflowInstanceRestartResponse restartWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @RequestBody WorkflowInstanceRestartRequest restartRequest) {
    RunRequest runRequest =
        RunRequest.builder()
            .requester(callerBuilder.build())
            .requestTime(restartRequest.getRequestTime())
            .requestId(restartRequest.getRequestId())
            .currentPolicy(restartRequest.getRestartPolicy())
            .runParams(new LinkedHashMap<>())
            // no runtimeTags, correlationId, or artifacts for manual step restart
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode(workflowId, workflowInstanceId, null)
                    .setRestartPolicy(restartRequest.getRestartPolicy())
                    .setDownstreamPolicy(restartRequest.getRestartPolicy())
                    .restartParams(
                        ObjectHelper.valueOrDefault(
                            restartRequest.getRunParams(), new LinkedHashMap<>()))
                    .build())
            .build();

    RunResponse runResponse = actionHandler.restart(runRequest);
    if (runResponse.getStatus() == RunResponse.Status.DELEGATED) {
      runResponse = stepActionHandler.restartDirectly(runResponse, runRequest, false);
    }
    return runResponse.toWorkflowRestartResponse();
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/actions/stop",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Stop a given workflow instance's latest run if it is in a non-terminal state")
  public WorkflowInstanceActionResponse stopWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId) {
    return actionHandler.stopLatest(workflowId, workflowInstanceId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/actions/kill",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Kill a given workflow instance's latest run if it is in a non-terminal state")
  public WorkflowInstanceActionResponse killWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId) {
    return actionHandler.killLatest(workflowId, workflowInstanceId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/actions/unblock",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Unblock a failed workflow instance due to the strict sequential run strategy")
  public WorkflowInstanceActionResponse unblockWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId) {
    return actionHandler.unblockLatest(workflowId, workflowInstanceId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/actions/stop",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Stop a given workflow instance run if it is in a non-terminal state")
  public WorkflowInstanceActionResponse stopWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId) {
    return actionHandler.stop(workflowId, workflowInstanceId, workflowRunId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/actions/kill",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Kill a given workflow instance run if it is in a non-terminal state")
  public WorkflowInstanceActionResponse killWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId) {
    return actionHandler.kill(workflowId, workflowInstanceId, workflowRunId, callerBuilder.build());
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/actions/unblock",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary = "Unblock a failed workflow instance run due to the strict sequential run strategy.")
  public WorkflowInstanceActionResponse unblockWorkflowInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId) {
    return actionHandler.unblock(
        workflowId, workflowInstanceId, workflowRunId, callerBuilder.build());
  }
}
