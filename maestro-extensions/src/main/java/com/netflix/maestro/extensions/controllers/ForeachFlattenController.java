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
package com.netflix.maestro.extensions.controllers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.models.StepIteration;
import com.netflix.maestro.extensions.models.StepIterationsSummary;
import com.netflix.maestro.extensions.utils.ForeachFlatteningHelper;
import com.netflix.maestro.extensions.utils.PaginationHelper;
import com.netflix.maestro.models.api.PaginationDirection;
import com.netflix.maestro.models.api.PaginationResult;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Controller class for foreach flattening rest operations. */
@SuppressFBWarnings("EI_EXPOSE_REP2")
@Tag(name = "api/v3/views/flatten", description = "Maestro Foreach Flatten APIs")
@Hidden
@RestController
@RequestMapping(
    value = "/api/v3/views/flatten",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class ForeachFlattenController {
  private static final int ITERATION_MAX_BATCH_LIMIT = 200;
  private static final int ITERATION_MIN_BATCH_LIMIT = 1;
  private static final TypeReference<Map<String, String>> TYPE_REF = new TypeReference<>() {};

  private final MaestroForeachFlattenedDao dao;
  private final ObjectMapper objectMapper;

  @Autowired
  public ForeachFlattenController(MaestroForeachFlattenedDao dao, ObjectMapper objectMapper) {
    this.dao = dao;
    this.objectMapper = objectMapper;
  }

  /**
   * Returns a {@link PaginationResult} of {@link StepIteration}s for the given step at particular
   * run.
   */
  @GetMapping(
      value =
          "/workflows/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/steps/{stepId}",
      consumes = MediaType.ALL_VALUE)
  public PaginationResult<StepIteration> getStepIterations(
      @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId,
      @PathVariable("stepId") String stepId,
      @Parameter(
              description =
                  "Number of results to return for the page, while paginating forward. Required and must"
                      + " be between 1 and 200 inclusive.")
          @RequestParam(name = "first", required = false)
          @Max(ITERATION_MAX_BATCH_LIMIT)
          @Min(ITERATION_MIN_BATCH_LIMIT)
          Integer first,
      @Parameter(
              description =
                  "Number of results to return for the page, while paginating backwards. Required and must"
                      + " be between 1 and 200 inclusive.")
          @RequestParam(name = "last", required = false)
          @Max(ITERATION_MAX_BATCH_LIMIT)
          @Min(ITERATION_MIN_BATCH_LIMIT)
          Integer last,
      @Parameter(
              description =
                  "Cursor used to identify the start for pagination "
                      + "If cursor is not provided or empty, we return the first or last page.")
          @RequestParam(name = "cursor", required = false)
          String cursor,
      @Parameter(
              description =
                  "A list of statuses to filter step iterations with desired statuses. "
                      + "For example: statuses=\"FATALLY_FAILED,TIMED_OUT\".")
          @RequestParam(name = "statuses", required = false)
          List<String> statuses,
      @Parameter(
              description =
                  "A json string that contains loop param key-value pairs to filter step "
                      + "iterations with desired loop params.")
          @RequestParam(name = "loopParams", required = false)
          String loopParamsStr) {

    PaginationDirection direction = PaginationHelper.validateParamAndDeriveDirection(first, last);
    int limit = (last == null) ? first : last;

    String iterationCursor = decodeCursor(cursor);
    Map<String, String> loopParameters = null;
    try {
      loopParameters = getLoopParameters(loopParamsStr);
    } catch (IOException ex) {
      LOG.error("Failed to unmarshal loop params: [{}]", loopParamsStr, ex);
    }

    List<StepIteration> stepIterations =
        dao.scanStepIterations(
            workflowId,
            workflowInstanceId,
            workflowRunId,
            stepId,
            iterationCursor,
            limit + 1, // request for 1 extra item
            direction.equals(PaginationDirection.NEXT),
            loopParameters,
            statuses);

    if (stepIterations.isEmpty()) {
      return PaginationHelper.buildEmptyPaginationResult();
    }
    int numElements = Math.min(stepIterations.size(), limit);
    boolean hasMoreItems = stepIterations.size() > limit;
    int retIndexStart = direction.equals(PaginationDirection.NEXT) ? 0 : hasMoreItems ? 1 : 0;
    int retIndexEnd =
        direction.equals(PaginationDirection.NEXT)
            ? numElements
            : hasMoreItems ? numElements + 1 : numElements;
    List<StepIteration> iterations = stepIterations.subList(retIndexStart, retIndexEnd);
    boolean hasNext = hasNext(direction, stepIterations.size() == limit + 1, cursor);
    boolean hasPrevious = hasPrevious(direction, stepIterations.size() == limit + 1, cursor);
    String forwardCursor = encodeCursor(iterations.get(numElements - 1).getIterationRank());
    String backCursor = encodeCursor(iterations.get(0).getIterationRank());

    return PaginationResult.<StepIteration>builder()
        .totalCount(numElements)
        .elements(iterations)
        .pageInfo(
            PaginationResult.PageInfo.builder()
                .startCursor(backCursor)
                .endCursor(forwardCursor)
                .hasNextPage(hasNext)
                .hasPreviousPage(hasPrevious)
                .build())
        .build();
  }

  @GetMapping(
      value =
          "/workflows/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/steps/{stepId}/iterations/{iterationId}",
      consumes = MediaType.ALL_VALUE)
  public StepIteration getStepIteration(
      @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId,
      @PathVariable("stepId") String stepId,
      @PathVariable("iterationId") String iterationId) {
    LOG.debug(
        "Received step iteration GET request: workflowId={}, workflowInstanceId={}, workflowRunId={}, step={}, iterationId={}",
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        iterationId);
    String iterationRank = ForeachFlatteningHelper.getIterationRank(iterationId);
    StepIteration stepIteration =
        dao.getStepIteration(workflowId, workflowInstanceId, workflowRunId, stepId, iterationRank);
    if (stepIteration == null) {
      throw new MaestroNotFoundException(
          "Step iteration is not found for workflowId=[%s], instanceId=[%s], runId=[%s], stepId=[%s], iterationId=[%s]",
          workflowId, workflowInstanceId, workflowRunId, stepId, iterationId);
    }
    return stepIteration;
  }

  private boolean hasNext(PaginationDirection direction, boolean hasMoreItems, String cursor) {
    if ((cursor == null || cursor.isEmpty()) && direction.equals(PaginationDirection.PREV)) {
      // means last page
      return false;
    }
    return direction.equals(PaginationDirection.PREV) || hasMoreItems;
  }

  private boolean hasPrevious(PaginationDirection direction, boolean hasMoreItems, String cursor) {
    if ((cursor == null || cursor.isEmpty()) && direction.equals(PaginationDirection.NEXT)) {
      // means first page
      return false;
    }
    // default case
    return direction.equals(PaginationDirection.NEXT) || hasMoreItems;
  }

  private Map<String, String> getLoopParameters(String loopParamsStr) throws IOException {
    if (loopParamsStr == null || loopParamsStr.isEmpty()) {
      return null;
    }
    return objectMapper.readValue(
        Base64.getUrlDecoder().decode(loopParamsStr.getBytes(Charset.defaultCharset())), TYPE_REF);
  }

  private String encodeCursor(String str) {
    return new String(
        Base64.getUrlEncoder().encode(str.getBytes(Charset.defaultCharset())),
        Charset.defaultCharset());
  }

  private String decodeCursor(@Nullable String str) {
    if (str == null || str.isEmpty()) {
      return null;
    }
    return new String(
        Base64.getUrlDecoder().decode(str.getBytes(Charset.defaultCharset())),
        Charset.defaultCharset());
  }

  /** Returns a {@link StepIterationsSummary} for the given step at particular run. */
  @GetMapping(
      value =
          "/workflows/{workflowId}/instances/{workflowInstanceId}/runs/{workflowRunId}/steps/{stepId}/summary",
      consumes = MediaType.ALL_VALUE)
  public StepIterationsSummary getStepIterationsSummary(
      @PathVariable("workflowId") String workflowId,
      @PathVariable("workflowInstanceId") long workflowInstanceId,
      @PathVariable("workflowRunId") long workflowRunId,
      @PathVariable("stepId") String stepId,
      @Parameter(
              description =
                  "Boolean to indicate whether to return count by status for eligible step iterations.")
          @RequestParam(name = "statusCount", defaultValue = "false")
          boolean statusCount,
      @Parameter(
              description =
                  "Boolean to indicate whether to return a single representative iteration. "
                      + "Default to true if not provided.")
          @RequestParam(name = "representative", defaultValue = "true")
          boolean representative,
      @Parameter(description = "Number of values to return for each loop param name.")
          @RequestParam(name = "valueCount", defaultValue = "5")
          Integer valueCount) {
    return dao.getStepIterationsSummary(
        workflowId,
        workflowInstanceId,
        workflowRunId,
        stepId,
        statusCount,
        representative,
        valueCount);
  }
}
