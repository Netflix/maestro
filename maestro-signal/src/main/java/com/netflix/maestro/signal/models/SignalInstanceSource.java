package com.netflix.maestro.signal.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import lombok.Data;

/**
 * Data model for the source of a signal instance.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
@Data
public class SignalInstanceSource {
  @Nullable private String workflowId;
  @Nullable private long workflowInstanceId;
  @Nullable private long workflowRunId;
  @Nullable private String stepId;
  @Nullable private long stepAttemptId;
  @Nullable private long stepInstanceId;

  /**
   * Helper method to create {@link SignalInstanceSource}.
   *
   * @param summary workflow summary
   * @param runtimeSummary step runtime summary
   * @return the signal instance source object
   */
  @JsonIgnore
  public static SignalInstanceSource create(
      WorkflowSummary summary, StepRuntimeSummary runtimeSummary) {
    SignalInstanceSource source = new SignalInstanceSource();
    source.setWorkflowId(summary.getWorkflowId());
    source.setWorkflowInstanceId(summary.getWorkflowInstanceId());
    source.setWorkflowRunId(summary.getWorkflowRunId());
    source.setStepId(runtimeSummary.getStepId());
    source.setStepInstanceId(runtimeSummary.getStepInstanceId());
    source.setStepAttemptId(runtimeSummary.getStepAttemptId());
    return source;
  }
}
