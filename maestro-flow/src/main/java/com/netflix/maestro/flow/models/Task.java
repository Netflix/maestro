package com.netflix.maestro.flow.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import lombok.Data;
import lombok.Getter;

/**
 * Task attempt data model. Business logics are defined over FlowTask interface.
 *
 * @author jun-he
 */
@Data
public class Task {

  @Getter
  public enum Status {
    /** in-progress Status. */
    IN_PROGRESS(false, true, false),
    /** canceled Status. */
    CANCELED(true, false, false),
    /** failed Status. */
    FAILED(true, false, true),
    /** failed with terminal error Status. */
    FAILED_WITH_TERMINAL_ERROR(true, false, false),
    /** completed Status. */
    COMPLETED(true, true, false),
    /** timed-out Status. */
    TIMED_OUT(true, false, true),
    /** skipped Status. */
    SKIPPED(true, true, false);

    private final boolean terminal;
    private final boolean successful;
    private final boolean restartable; // manual restartable

    Status(boolean terminal, boolean successful, boolean restartable) {
      this.terminal = terminal;
      this.successful = successful;
      this.restartable = restartable;
    }
  }

  // immutable runtime states
  private String taskId;
  private TaskDef taskDef;
  private long seq;

  // mutable runtime states
  private Status status;
  private String reasonForIncompletion;
  private Map<String, Object> outputData;
  private long startDelayInSeconds; // task polling interval
  private long retryCount;
  private int pollCount;
  private Long startTime; // used to record the execution start time
  private Long timeoutInMillis; // keep unset timeout value from maestro engine

  @JsonIgnore
  public String referenceTaskName() {
    return taskDef.taskReferenceName();
  }

  @JsonIgnore
  public Map<String, Object> getInputData() {
    return taskDef.input();
  }

  @JsonIgnore
  public String getTaskType() {
    return taskDef.type();
  }

  @JsonIgnore
  public void incrementPollCount() {
    pollCount++;
  }

  @JsonIgnore
  public boolean isTerminal() {
    return status.isTerminal();
  }
}
