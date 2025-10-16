package com.netflix.maestro.flow.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.flow.Constants;
import java.util.Map;
import lombok.Data;
import lombok.Getter;

/**
 * Task attempt data model. Business logics are defined over FlowTask interface. Note that all the
 * data/state in the task class is local and not thread safe.
 *
 * <p>Task active flag is important to support certain cases to run dummy tasks. In those cases, the
 * dummy task owned by a flow might take actions but should not execute tsk (FlowTask's execute)
 * because of race conditions. The flow must activate the inactive child task actor first before the
 * task actor can switch to execute. So those inactive tasks are not real maestro tasks. This is
 * required to avoid that the child actor runs the business logic but the parent flow is unaware and
 * decide to finish. Also, the active flag is a local state and not thread safe and can only be
 * accessed within the actor, e.g. flow owns a list of copied tasks, and it can mutate active flag
 * for its own snapshots.
 *
 * <p>Basic rule: flow actor can only activate a task actor. A task actor can only deactivate itself
 * and inform its parent flow actor. Only the task actor itself can set the started flag to true
 * after proper initialization.
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
    CANCELED(true, false, true),
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
  private long startDelayInMillis; // task polling interval
  private long retryCount;
  private int pollCount;
  private boolean active = true; // flag to indicate if a running task is active or not
  private boolean started; // flag to indicate if a running task is started or not
  private int code = Constants.TASK_PING_CODE; // wakeup code for custom signaling
  private Long startTime; // used to record the execution start time
  private Long timeoutInMillis; // keep unset timeout value from maestro engine
  private Long endTime; // used to record the execution end time

  @JsonIgnore
  public String referenceTaskName() {
    return taskDef.taskReferenceName();
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
