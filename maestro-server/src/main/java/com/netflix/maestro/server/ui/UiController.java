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
package com.netflix.maestro.server.ui;

import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.dto.MaestroWorkflow;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowOverviewResponse;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.WorkflowTimeline;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Read-only operator UI for Maestro, server-rendered with Thymeleaf and served under {@code /ui}.
 *
 * <p>It reads through the same DAOs the REST controllers use. Local {@link ExceptionHandler}
 * methods render friendly HTML error pages; because controller-local handlers take precedence over
 * the global {@code @ControllerAdvice}, UI errors are not returned as JSON.
 */
@Slf4j
@Controller
@RequestMapping("/ui")
public class UiController {
  private static final int WORKFLOW_PAGE_SIZE = 50;
  private static final int RECENT_VERSIONS = 10;
  private static final int RECENT_INSTANCES = 50;

  private static final String ATTR_WORKFLOW_ID = "workflowId";
  private static final String ATTR_INSTANCE_ID = "instanceId";
  private static final String ATTR_STATUS = "status";
  private static final String ATTR_TITLE = "title";
  private static final String ATTR_MESSAGE = "message";
  private static final String ERROR_VIEW = "ui/error";

  private final MaestroWorkflowDao workflowDao;
  private final MaestroWorkflowInstanceDao workflowInstanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;

  /** Constructor. */
  public UiController(
      MaestroWorkflowDao workflowDao,
      MaestroWorkflowInstanceDao workflowInstanceDao,
      MaestroStepInstanceDao stepInstanceDao) {
    this.workflowDao = workflowDao;
    this.workflowInstanceDao = workflowInstanceDao;
    this.stepInstanceDao = stepInstanceDao;
  }

  /** Landing page: redirect to the workflows list. */
  @GetMapping({"", "/"})
  public String home() {
    return "redirect:/ui/workflows";
  }

  /** List workflows with a simple internal-id cursor. */
  @GetMapping("/workflows")
  public String workflows(
      @RequestParam(name = "cursor", defaultValue = "0") long cursor, Model model) {
    List<MaestroWorkflow> workflows = workflowDao.scanWorkflows(cursor, WORKFLOW_PAGE_SIZE);
    Long nextCursor = null;
    if (workflows.size() == WORKFLOW_PAGE_SIZE) {
      nextCursor = workflows.get(workflows.size() - 1).getInternalId();
    }
    model.addAttribute("workflows", workflows);
    model.addAttribute("cursor", cursor);
    model.addAttribute("nextCursor", nextCursor);
    return "workflows/list";
  }

  /** Workflow detail: overview, recent versions, recent instances, and change timeline. */
  @GetMapping("/workflows/{workflowId}")
  public String workflow(@PathVariable(ATTR_WORKFLOW_ID) String workflowId, Model model) {
    WorkflowOverviewResponse overview = workflowDao.getWorkflowOverview(workflowId);
    WorkflowTimeline timeline = workflowDao.getWorkflowTimeline(workflowId);
    List<WorkflowDefinition> versions =
        workflowDao.scanWorkflowDefinition(workflowId, 0, RECENT_VERSIONS);
    List<WorkflowInstance> instances = recentInstances(workflowId);

    model.addAttribute(ATTR_WORKFLOW_ID, workflowId);
    model.addAttribute("overview", overview);
    model.addAttribute("timeline", timeline);
    model.addAttribute("versions", versions);
    model.addAttribute("instances", instances);
    return "workflows/detail";
  }

  /** Workflow instance detail: instance view overlaid across runs plus its step instances. */
  @GetMapping("/workflows/{workflowId}/instances/{instanceId}")
  public String instance(
      @PathVariable(ATTR_WORKFLOW_ID) String workflowId,
      @PathVariable(ATTR_INSTANCE_ID) long instanceId,
      Model model) {
    WorkflowInstance instance =
        workflowInstanceDao.getWorkflowInstance(
            workflowId, instanceId, Constants.LATEST_INSTANCE_RUN, true);
    instance.enrich();
    List<StepInstance> steps = stepInstanceDao.getAllStepInstanceViews(workflowId, instanceId);
    steps.sort(Comparator.comparingLong(StepInstance::getStepInstanceId));

    model.addAttribute(ATTR_WORKFLOW_ID, workflowId);
    model.addAttribute(ATTR_INSTANCE_ID, instanceId);
    model.addAttribute("instance", instance);
    model.addAttribute("steps", steps);
    return "instances/detail";
  }

  /** Step instance detail overlaid across all attempts of all runs. */
  @GetMapping("/workflows/{workflowId}/instances/{instanceId}/steps/{stepId}")
  public String step(
      @PathVariable(ATTR_WORKFLOW_ID) String workflowId,
      @PathVariable(ATTR_INSTANCE_ID) long instanceId,
      @PathVariable("stepId") String stepId,
      Model model) {
    StepInstance step = stepInstanceDao.getStepInstanceView(workflowId, instanceId, stepId);
    step.enrich();

    model.addAttribute(ATTR_WORKFLOW_ID, workflowId);
    model.addAttribute(ATTR_INSTANCE_ID, instanceId);
    model.addAttribute("stepId", stepId);
    model.addAttribute("step", step);
    return "steps/detail";
  }

  private List<WorkflowInstance> recentInstances(String workflowId) {
    long[] minMax = workflowInstanceDao.getMinMaxWorkflowInstanceIds(workflowId);
    if (minMax == null) {
      return List.of();
    }
    long earliest = minMax[0];
    long latest = minMax[1];
    long start = Math.max(earliest, latest - RECENT_INSTANCES + 1);
    return workflowInstanceDao.getWorkflowInstancesWithLatestRun(workflowId, start, latest, true);
  }

  /** Render a friendly 404 page for missing workflows/instances/steps. */
  @ExceptionHandler(MaestroNotFoundException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public String handleNotFound(MaestroNotFoundException ex, Model model) {
    model.addAttribute(ATTR_STATUS, HttpStatus.NOT_FOUND.value());
    model.addAttribute(ATTR_TITLE, "Not found");
    model.addAttribute(ATTR_MESSAGE, ex.getMessage());
    return ERROR_VIEW;
  }

  /** Render a friendly error page for any other Maestro runtime error in the UI. */
  @ExceptionHandler(MaestroRuntimeException.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public String handleRuntime(MaestroRuntimeException ex, Model model) {
    LOG.warn("UI request failed", ex);
    model.addAttribute(ATTR_STATUS, HttpStatus.INTERNAL_SERVER_ERROR.value());
    model.addAttribute(ATTR_TITLE, "Something went wrong");
    model.addAttribute(ATTR_MESSAGE, ex.getMessage());
    return ERROR_VIEW;
  }
}
