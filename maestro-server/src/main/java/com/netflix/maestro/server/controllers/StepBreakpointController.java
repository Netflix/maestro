package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.dao.MaestroStepBreakpointDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.StepBreakpointCreateRequest;
import com.netflix.maestro.models.api.StepBreakpointDeleteResponse;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.stepruntime.PausedStepAttempt;
import com.netflix.maestro.models.stepruntime.StepBreakpoint;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API controller for managing step breakpoints in Maestro workflows. Breakpoints allow
 * developers to pause workflow execution at specific steps for debugging, inspection, or manual
 * intervention purposes.
 *
 * <p>This controller provides endpoints to add breakpoints at various levels:
 *
 * <ul>
 *   <li>Step level - affects all instances and runs of a step
 *   <li>Step definition level - affects all instances and runs of a step for a specific workflow
 *       version
 *   <li>Step instance level - affects a specific step instance run and all its attempts
 *   <li>Step attempt level - affects a specific step attempt
 * </ul>
 *
 * @see StepBreakpoint
 * @see MaestroStepBreakpointDao
 */
@Tag(
    name = "/api/v3/workflows/steps/breakpoints",
    description = "Maestro StepBreakpoint controller APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows/steps/breakpoints",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class StepBreakpointController {
  private final MaestroStepBreakpointDao stepBreakpointDao;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final User.UserBuilder callerBuilder;

  /**
   * Constructor for dependency injection.
   *
   * @param instanceDao workflow instance DAO for retrieving instance information
   * @param stepBreakpointDao step breakpoint DAO for managing breakpoints
   * @param callerBuilder user builder to construct the caller user information
   */
  @Autowired
  public StepBreakpointController(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepBreakpointDao stepBreakpointDao,
      User.UserBuilder callerBuilder) {
    this.instanceDao = instanceDao;
    this.stepBreakpointDao = stepBreakpointDao;
    this.callerBuilder = callerBuilder;
  }

  /** Add a breakpoint corresponding to a step definition. */
  @Operation(summary = "Add a breakpoint corresponding to a step definition")
  @PostMapping(value = "", consumes = MediaType.APPLICATION_JSON_VALUE)
  public StepBreakpoint addBreakpoint(
      @Valid @NotNull @RequestBody StepBreakpointCreateRequest request) {
    validateStepBreakpointRequest(request);
    return stepBreakpointDao.addStepBreakpoint(
        request.getWorkflowId(),
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        request.getStepId(),
        Constants.MATCH_ALL_STEP_ATTEMPTS,
        callerBuilder.build());
  }

  /**
   * Removes a breakpoint for a step definition and optionally resumes paused steps.
   *
   * <p>When resume is true, all step attempts currently paused by this breakpoint will be resumed.
   * This operation can be expensive if many steps are paused, but is bounded by the maximum step
   * concurrency limit.
   *
   * @param workflowId the workflow identifier
   * @param stepId the step identifier
   * @param resume whether to resume paused steps (default: true)
   * @return response containing deletion details and number of resumed steps
   */
  @DeleteMapping(value = "", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Remove step breakpoint corresponding to a step definition")
  public StepBreakpointDeleteResponse removeBreakpoint(
      @RequestParam(name = "workflowId") String workflowId,
      @RequestParam(name = "stepId") String stepId,
      @RequestParam(name = "resume", required = false, defaultValue = "true") boolean resume) {
    int resumedSteps =
        stepBreakpointDao.removeStepBreakpoint(
            workflowId,
            Constants.MATCH_ALL_WORKFLOW_VERSIONS,
            Constants.MATCH_ALL_WORKFLOW_INSTANCES,
            Constants.MATCH_ALL_RUNS,
            stepId,
            Constants.MATCH_ALL_STEP_ATTEMPTS,
            resume);
    return StepBreakpointDeleteResponse.builder()
        .workflowId(workflowId)
        .stepId(stepId)
        .timelineEvent(
            TimelineLogEvent.builder()
                .level(TimelineEvent.Level.INFO)
                .message(
                    "StepBreakpoint has been removed by the user [%s]. Total of [%d] steps have been resumed",
                    callerBuilder.build(), resumedSteps)
                .build())
        .build();
  }

  /**
   * Retrieves all breakpoints for a step definition.
   *
   * <p>This endpoint returns all active breakpoints that would cause the specified step to pause
   * during execution.
   *
   * @param workflowId the workflow identifier
   * @param stepId the step identifier
   * @return list of active breakpoints for the step
   */
  @GetMapping(value = "", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get step breakpoints corresponding to an identifier")
  public List<StepBreakpoint> getBreakpoints(
      @RequestParam(name = "workflowId") String workflowId,
      @RequestParam(name = "stepId") String stepId) {
    return stepBreakpointDao.getStepBreakPoints(
        workflowId,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        stepId,
        Constants.MATCH_ALL_STEP_ATTEMPTS);
  }

  /** Gets all the step instance attempts "currently paused" due to breakpoint. */
  @GetMapping(value = "/paused-step-attempts", consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Get all the currently paused step instance attempts due to the step definition breakpoint")
  public List<PausedStepAttempt> getPausedStepInstanceAttempts(
      @RequestParam(name = "workflowId") String workflowId,
      @RequestParam(name = "stepId") String stepId) {
    return stepBreakpointDao.getPausedStepAttempts(
        workflowId,
        Constants.MATCH_ALL_WORKFLOW_VERSIONS,
        Constants.MATCH_ALL_WORKFLOW_INSTANCES,
        Constants.MATCH_ALL_RUNS,
        stepId,
        Constants.MATCH_ALL_STEP_ATTEMPTS);
  }

  @PutMapping(
      value =
          "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRun}/steps/{stepId}/"
              + "attempts/{stepAttemptId}/actions/resume")
  public StepInstanceActionResponse resumePausedStepAttempt(
      @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRun") String workflowRun,
      @PathVariable("stepId") String stepId,
      @PathVariable("stepAttemptId") long stepAttemptId) {
    WorkflowInstance instance =
        instanceDao.getWorkflowInstance(workflowId, workflowInstanceId, workflowRun, false);
    stepBreakpointDao.resumePausedStepInstanceAttempt(
        workflowId,
        instance.getWorkflowVersionId(),
        workflowInstanceId,
        instance.getWorkflowRunId(),
        stepId,
        stepAttemptId);
    return StepInstanceActionResponse.builder()
        .workflowId(workflowId)
        .workflowInstanceId(workflowInstanceId)
        .workflowRunId(instance.getWorkflowRunId())
        .stepId(stepId)
        .stepAttemptId(stepAttemptId)
        .timelineEvent(
            TimelineLogEvent.info(
                "User [%s] take action [%s] on the step [%s]",
                callerBuilder.build().getName(), Actions.StepInstanceAction.RESUME, stepId))
        .build();
  }

  /**
   * Validates the step breakpoint request to ensure only supported fields are provided.
   *
   * <p>Currently, only workflow and step IDs are supported. Version, instance, run, and
   * attempt-specific breakpoints are not yet supported.
   *
   * @param request the breakpoint creation request to validate
   * @throws IllegalArgumentException if unsupported fields are provided
   */
  private void validateStepBreakpointRequest(StepBreakpointCreateRequest request) {
    Checks.checkTrue(
        request.getWorkflowVersion() == null
            && request.getWorkflowInstanceId() == null
            && request.getWorkflowRun() == null
            && request.getStepAttemptId() == null,
        "workflowVersion, workflowInstanceId, workflowRunId or stepAttemptId can't be specified for"
            + " step breakpoint. Request [%s]",
        request);
  }
}
