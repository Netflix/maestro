package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.handlers.StepInstanceActionHandler;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.api.StepInstanceRestartRequest;
import com.netflix.maestro.models.api.StepInstanceRestartResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.utils.ObjectHelper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Step instance action related REST API.
 *
 * <p>All the actions for a step instance are applied to the latest step attempt in the latest
 * workflow instance run as all other attempts have already een in one of immutable terminal states.
 *
 * @author jun-he
 */
@Tag(name = "/api/v3/workflows", description = "Maestro Workflow Step Instance Action APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class StepInstanceActionController {

  private final StepInstanceActionHandler stepActionHandler;
  private final User.UserBuilder callerBuilder;

  @Autowired
  public StepInstanceActionController(
      StepInstanceActionHandler stepActionHandler, User.UserBuilder callerBuilder) {
    this.stepActionHandler = stepActionHandler;
    this.callerBuilder = callerBuilder;
  }

  @PostMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}/actions/restart",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Restart a step in terminal status and continue the workflow execution. "
              + "If the workflow is still running, it adds a new attempt, otherwise, "
              + "it creates a new workflow instance run. The returned response will include "
              + "the workflow/step info related to the first restart point along the restart chain.")
  public StepInstanceRestartResponse restartStepInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @Valid @NotNull @RequestBody StepInstanceRestartRequest restartRequest,
      @RequestParam(value = "blocking", defaultValue = "true") boolean blocking) {
    RunRequest runRequest =
        RunRequest.builder()
            .requester(callerBuilder.build())
            .requestTime(restartRequest.getRequestTime())
            .requestId(restartRequest.getRequestId())
            .currentPolicy(restartRequest.getRestartRunPolicyWithUpstreamRestartMode())
            .runParams(new LinkedHashMap<>())
            // no runtimeTags, correlationId, or artifacts for manual step restart
            .restartConfig(
                RestartConfig.builder()
                    // initial restart node and might be overridden in upstream restart mode
                    .addRestartNode(workflowId, workflowInstanceId, stepId)
                    .restartPolicy(restartRequest.getRestartRunPolicyWithUpstreamRestartMode())
                    .setDownstreamPolicy(restartRequest.getRestartPolicy())
                    .stepRestartParams(
                        Collections.singletonMap(
                            stepId,
                            ObjectHelper.valueOrDefault(
                                restartRequest.getStepRunParams(), new LinkedHashMap<>())))
                    .build())
            .build();

    RunResponse runResponse = stepActionHandler.restart(runRequest, blocking);
    return runResponse.toStepRestartResponse();
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}/actions/stop",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Stop a given step instance's current attempt")
  public StepInstanceActionResponse stopStepInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @RequestParam(value = "blocking", defaultValue = "true") boolean blocking) {
    return stepActionHandler.terminate(
        workflowId,
        workflowInstanceId,
        stepId,
        callerBuilder.build(),
        Actions.StepInstanceAction.STOP,
        blocking);
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}/actions/kill",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Kill a given step instance's current attempt")
  public StepInstanceActionResponse killStepInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @RequestParam(value = "blocking", defaultValue = "true") boolean blocking) {
    return stepActionHandler.terminate(
        workflowId,
        workflowInstanceId,
        stepId,
        callerBuilder.build(),
        Actions.StepInstanceAction.KILL,
        blocking);
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}/actions/skip",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Skip a given step instance's current attempt. "
              + "If the workflow instance is not running, this will restart the step and then skip it.")
  public StepInstanceActionResponse skipStepInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @RequestParam(value = "blocking", defaultValue = "true") boolean blocking) {
    RunRequest runRequest =
        RunRequest.builder()
            .requester(callerBuilder.build())
            .currentPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .runParams(new LinkedHashMap<>())
            // no runtimeTags, correlationId, or artifacts for manual step restart
            .restartConfig(
                RestartConfig.builder()
                    .addRestartNode(workflowId, workflowInstanceId, stepId)
                    .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
                    .skipSteps(Collections.singleton(stepId))
                    .stepRestartParams(Collections.singletonMap(stepId, Collections.emptyMap()))
                    .build())
            .build();
    return stepActionHandler.skip(
        workflowId, workflowInstanceId, stepId, callerBuilder.build(), runRequest, blocking);
  }

  @PutMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}/actions/bypass",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Bypass the step dependencies for a given step instance's current attempt")
  public StepInstanceActionResponse bypassStepDependencies(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @RequestParam(value = "blocking", defaultValue = "true") boolean blocking) {
    // dependencies can be signal or other types of dependencies as well
    return stepActionHandler.bypassStepDependencies(
        workflowId, workflowInstanceId, stepId, callerBuilder.build(), blocking);
  }
}
