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
package com.netflix.maestro.engine.stepruntime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.http.HttpRuntimeExecutor;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.artifact.HttpArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.stepruntime.HttpStepRequest;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro HTTP step runtime.
 *
 * <p>Executes HTTP requests based on the parameters defined in default-http-step-params.yaml.
 */
@Slf4j
@AllArgsConstructor
public class HttpStepRuntime implements StepRuntime {
  private static final String HTTP_PARAM_KEY = "http";
  private static final String STATUS_CODE_PARAM_KEY = "status_code";
  private static final String RESPONSE_BODY_PARAM_KEY = "response_body";
  private static final String STATE_EXPR_PARAM_KEY = "state_expr";

  private final HttpRuntimeExecutor executor;
  private final JobTemplateManager jobTemplateManager;
  private final OutputDataManager outputDataManager;
  private final ParamEvaluator paramEvaluator;
  private final ObjectMapper objectMapper;

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    var stepRef = workflowSummary.getIdentity() + runtimeSummary.getIdentity();
    try {
      MapParameter mapParams = getAndValidateHttpParams(runtimeSummary);
      HttpStepRequest request =
          objectMapper.convertValue(mapParams.getEvaluatedResult(), HttpStepRequest.class);
      List<TimelineEvent> timeline = new ArrayList<>();
      LOG.debug(
          "Making the HTTP request: method=[{}], url=[{}] for a step instance {}",
          request.getMethod(),
          request.getUrl(),
          stepRef);
      timeline.add(
          TimelineLogEvent.info(
              "Making the HTTP request: method=[%s], url=[%s]",
              request.getMethod(), request.getUrl()));

      HttpResponse<String> response = executor.execute(request);

      LOG.debug(
          "The HTTP request completed with status code=[{}] for a step instance {}",
          response.statusCode(),
          stepRef);
      timeline.add(
          TimelineLogEvent.info(
              "The HTTP request completed with status code=[%s]", response.statusCode()));

      HttpArtifact artifact = createHttpArtifact(request, response);
      var outputParams = outputParams(workflowSummary, runtimeSummary, artifact);
      State state = deriveState(workflowSummary, runtimeSummary, outputParams);
      artifact.setStatus(state.isFailed() ? "FAILED" : "SUCCESS");

      timeline.add(
          TimelineLogEvent.info(
              "HTTP step finished with state [%s] based on the SEL evaluation of "
                  + STATE_EXPR_PARAM_KEY,
              state.name()));
      return new Result(state, Map.of(artifact.getType().key(), artifact), timeline);
    } catch (MaestroRetryableError mre) {
      LOG.info("Failed to execute http step runtime, will retry", mre);
      return new Result(
          State.CONTINUE,
          Collections.emptyMap(),
          Collections.singletonList(TimelineDetailsEvent.from(mre.getDetails())));
    } catch (Exception e) {
      LOG.error("Failed to execute http step runtime for a step instance {}", stepRef, e);

      var state =
          switch (e) {
            case InterruptedException ignored -> {
              Thread.currentThread().interrupt();
              yield State.PLATFORM_ERROR;
            }
            case IOException ignored -> State.PLATFORM_ERROR;
            default -> State.FATAL_ERROR;
          };

      return new Result(
          state,
          Collections.emptyMap(),
          Collections.singletonList(
              TimelineDetailsEvent.from(
                  Details.create(e, false, "Failed to execute http step runtime with an error"))));
    }
  }

  /** Derive the http step state based on the state_expr SEL expression evaluated result. */
  private State deriveState(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      Map<String, Parameter> outputParams) {
    Map<String, Parameter> allStepParams = new LinkedHashMap<>(runtimeSummary.getParams());
    allStepParams.putAll(outputParams);
    StringParameter stateParam =
        StringParameter.builder()
            .name(STATE_EXPR_PARAM_KEY)
            .expression(
                runtimeSummary
                    .getParams()
                    .get(STATE_EXPR_PARAM_KEY)
                    .asStringParam()
                    .getEvaluatedResult())
            .build();
    paramEvaluator.parseStepParameter(
        Collections.emptyMap(),
        workflowSummary.getParams(),
        allStepParams,
        stateParam,
        runtimeSummary.getStepId());

    return State.valueOf(stateParam.getEvaluatedResult().toUpperCase(Locale.US));
  }

  /**
   * Get and validate http parameters.
   *
   * @param stepSummary step runtime summary
   * @return http parameters
   */
  private MapParameter getAndValidateHttpParams(StepRuntimeSummary stepSummary) {
    Checks.notNull(stepSummary.getParams(), "params must be present");
    Checks.notNull(stepSummary.getParams().get(HTTP_PARAM_KEY), "http params must be present");
    return stepSummary.getParams().get(HTTP_PARAM_KEY).asMapParam();
  }

  /**
   * Create HTTP artifact from request and response.
   *
   * @param request the HTTP request
   * @param response the HTTP response
   * @return HttpArtifact object
   */
  private HttpArtifact createHttpArtifact(HttpStepRequest request, HttpResponse<String> response) {
    HttpArtifact artifact = new HttpArtifact();
    artifact.setRequest(request);
    artifact.setStatusCode(response.statusCode());
    artifact.setBody(response.body());
    return artifact;
  }

  /**
   * Create output params and save it using output data manager.
   *
   * @param workflowSummary workflow summary
   * @param runtimeSummary step runtime summary
   * @param artifact http artifact
   * @return output params
   */
  private Map<String, Parameter> outputParams(
      WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary, HttpArtifact artifact) {
    Map<String, Parameter> outputParams = new LinkedHashMap<>();
    long statusCode = artifact.getStatusCode();
    outputParams.put(
        STATUS_CODE_PARAM_KEY,
        LongParameter.builder()
            .name(STATUS_CODE_PARAM_KEY)
            .value(statusCode)
            .evaluatedResult(statusCode)
            .evaluatedTime(System.currentTimeMillis())
            .build());
    outputParams.put(
        RESPONSE_BODY_PARAM_KEY,
        StringParameter.builder()
            .name(RESPONSE_BODY_PARAM_KEY)
            .value(artifact.getBody())
            .evaluatedResult(artifact.getBody())
            .evaluatedTime(System.currentTimeMillis())
            .build());
    outputDataManager.saveOutputData(
        new OutputData(
            runtimeSummary.getType(),
            runtimeSummary.getStepInstanceUuid(),
            workflowSummary.getWorkflowId(),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            outputParams,
            Collections.emptyMap()));
    return outputParams;
  }

  @Override
  public Map<String, ParamDefinition> injectRuntimeParams(
      WorkflowSummary workflowSummary, Step step) {
    return jobTemplateManager.loadRuntimeParams(workflowSummary, step);
  }

  @Override
  public List<Tag> injectRuntimeTags(WorkflowSummary workflowSummary, Step step) {
    return jobTemplateManager.loadTags(workflowSummary, step);
  }
}
