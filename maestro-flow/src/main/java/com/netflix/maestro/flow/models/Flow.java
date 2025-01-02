package com.netflix.maestro.flow.models;

import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import lombok.Data;
import lombok.Getter;

/**
 * Flow instance data model.
 *
 * @author jun-he
 */
@Data
public class Flow {
  @Getter
  public enum Status {
    /** running Status. */
    RUNNING(false, false),
    /** completed Status. */
    COMPLETED(true, true),
    /** failed Status. */
    FAILED(true, false),
    /** timed-out Status. */
    TIMED_OUT(true, false),
    /** terminated Status. */
    TERMINATED(true, false);

    private final boolean terminal;
    private final boolean successful;

    Status(boolean terminal, boolean successful) {
      this.terminal = terminal;
      this.successful = successful;
    }
  }

  // immutable data to persist to db
  private final long groupId;
  private final String flowId;
  private final long generation;
  private final long startTime;
  private final String reference;

  // transient immutable data
  private Map<String, Object> input;
  private FlowDef flowDef;

  // transient mutable data
  private volatile Status status;
  private long updateTime;
  private String reasonForIncompletion;
  private long seq = 0;

  private Task prepareTask; // inline task runs at the beginning before user jobs
  private Task monitorTask; // inline task runs whenever there is an update in the flow

  private final Map<String, Task> finishedTasks = new HashMap<>();
  private final Map<String, Task> runningTasks = new HashMap<>();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();

  /**
   * Add a finished task to the finished task map. FinishedTasks map only keeps the latest attempt
   * per task ref name. It might contain the dummy tasks.
   *
   * @param task task to add
   */
  public void addFinishedTask(Task task) {
    Checks.checkTrue(task.isTerminal(), "task [%s] must be in the terminal state", task);
    try {
      writeLock.lock();
      finishedTasks.put(task.referenceTaskName(), task);
    } finally {
      writeLock.unlock();
    }
    runningTasks.remove(task.referenceTaskName());
  }

  /**
   * Update a running task in the running task map. RunningTasks map only keeps the latest attempt
   * per task ref name. It will contain the dummy tasks. As running tasks won't be accessed outside
   * of flow actor. There is no lock needed.
   *
   * @param task task to add
   */
  public void updateRunningTask(Task task) {
    runningTasks.put(task.referenceTaskName(), task);
  }

  /** This is thread safe and can be called by flow's task actors directly. */
  public Collection<Task> getFinishedTasks() {
    try {
      readLock.lock();
      return new ArrayList<>(finishedTasks.values());
    } finally {
      readLock.unlock();
    }
  }

  /** This is not thread safe and can only be called by the flow actor itself. */
  public Stream<Task> getStreamOfAllTasks() {
    return Stream.concat(finishedTasks.values().stream(), runningTasks.values().stream());
  }

  public long getTimeoutInMillis() {
    return flowDef.getTimeoutInMillis();
  }

  public void markTimedout() {
    this.status = Status.TIMED_OUT;
    markUpdate();
  }

  public void markUpdate() {
    this.updateTime = System.currentTimeMillis();
  }

  /**
   * create a new task attempt based on latest task info.
   *
   * @param def task def
   * @param inline flag to indicate if the task is actually run by the flow actor
   * @return created task
   */
  public Task newTask(TaskDef def, boolean inline) {
    Task task = new Task();
    task.setTaskId(UUID.randomUUID().toString());
    task.setTaskDef(def);
    task.setSeq(++seq);
    task.setStatus(Task.Status.IN_PROGRESS);
    task.setReasonForIncompletion(null);
    task.setOutputData(new HashMap<>());
    task.setStartDelayInSeconds(0);
    task.setRetryCount(0);
    task.setPollCount(0);
    // task startTime should be set when execution start, not creation time
    if (!inline) {
      runningTasks.put(def.taskReferenceName(), task);
    }
    return task;
  }
}
