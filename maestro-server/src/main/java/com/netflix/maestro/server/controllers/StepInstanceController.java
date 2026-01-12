package com.netflix.maestro.server.controllers;

import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.instance.StepInstance;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Step instance related REST API.
 *
 * @author jun-he
 */
@Tag(name = "/api/v3/workflows", description = "Maestro Workflow Step Instance APIs")
@RestController
@RequestMapping(
    value = "/api/v3/workflows",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
public class StepInstanceController {

  private final MaestroStepInstanceDao stepInstanceDao;

  @Autowired
  public StepInstanceController(MaestroStepInstanceDao stepInstanceDao) {
    this.stepInstanceDao = stepInstanceDao;
  }

  @GetMapping(value = "/steps/instances/action-map", consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get step instance status to step instance action mapping")
  public Map<StepInstance.Status, List<Actions.StepInstanceAction>> getStepInstanceActionMap() {
    return Actions.STEP_INSTANCE_STATUS_TO_ACTION_MAP;
  }

  @GetMapping(
      value = "/{workflowId}/instances/{workflowInstanceId}/steps/{stepId}",
      consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get a step instance view by overlaying data from all attempts of all runs")
  public StepInstance getStepInstanceView(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @RequestParam(name = "enriched", defaultValue = "true") boolean enriched) {
    StepInstance instance =
        stepInstanceDao.getStepInstanceView(workflowId, workflowInstanceId, stepId);
    if (enriched) {
      instance.enrich();
    }
    return instance;
  }

  @GetMapping(
      value =
          "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/steps/{stepId}/attempts/{attempt}",
      consumes = MediaType.ALL_VALUE)
  @Operation(
      summary =
          "Get a specific step instance info for a given step instance id and "
              + "attempt info (i.e. latest, or exact attempt id)")
  public StepInstance getStepInstance(
      @Valid @NotNull @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId,
      @Valid @NotNull @PathVariable("stepId") String stepId,
      @Valid @NotNull @PathVariable("attempt") String attempt,
      @RequestParam(name = "enriched", defaultValue = "true") boolean enriched) {
    StepInstance instance =
        stepInstanceDao.getStepInstance(
            workflowId, workflowInstanceId, workflowRunId, stepId, attempt);
    if (enriched) {
      instance.enrich();
    }
    return instance;
  }

  @GetMapping(
          value = "/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/steps",
          consumes = MediaType.ALL_VALUE)
  @Operation(summary = "Get all step instance views in a given workflow instance run")
  public List<StepInstance> getStepInstanceViews(
          @PathVariable("workflowId") String workflowId,
          @PathVariable("workflowInstanceId") long workflowInstanceId,
          @PathVariable("workflowRunId") long workflowRunId) {
    // no need pagination as the list size is at most Constants.STEP_LIST_SIZE_LIMIT (300)
    List<StepInstance> stepInstances =
            stepInstanceDao.getAllStepInstances(workflowId, workflowInstanceId, workflowRunId);
    // todo optimize the query if needed for performance concerns
    List<StepInstance> stepInstanceViews =
            new ArrayList<>(
                    stepInstances.stream()
                            .collect(
                                    Collectors.toMap(
                                            StepInstance::getStepInstanceId,
                                            Function.identity(),
                                            (s1, s2) -> s1.getStepAttemptId() > s2.getStepAttemptId() ? s1 : s2))
                            .values());
    stepInstanceViews.sort(Comparator.comparingLong(StepInstance::getStepInstanceId));
    return stepInstanceViews;
  }
}
