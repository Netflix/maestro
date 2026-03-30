package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Class that stores a summary of a single workflow instance run. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_run_id", "status", "start_time", "end_time"},
    alphabetic = true)
@JsonDeserialize(builder = WorkflowRunSummary.WorkflowRunSummaryBuilder.class)
@Builder
@Getter
@EqualsAndHashCode
public class WorkflowRunSummary {
  private final long workflowRunId;
  @NotNull private final WorkflowInstance.Status status;
  private final Long startTime;
  private final Long endTime;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class WorkflowRunSummaryBuilder {}
}
