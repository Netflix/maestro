package com.netflix.maestro.flow.actor;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Flow actor is responsible to run through the whole flow and manage all tasks it launches. It is
 * highly optimized for Maestro engine, although it is loosely coupled with maestro engine. Most of
 * the states of flow actors are in memory and safe to be lost without any issue. The state in DB is
 * always the source of truth and in memory state is just an in-sync copy or derived variation. It
 * assumes all the tasks can be collocated in the same node. We can improve it to support super
 * large flow by breaking a flow into sub-flows. Note that maestro engine already supports super
 * large flows already. Maestro engine will break a large flow into reasonable size flows. So flow
 * engine can assume tasks are collocated. Additionally, task actors will pass a small portion of
 * necessary data back for sharing with all other running task actors.
 *
 * <p>Currently, flow engine supports the retry interval with some small existing issues in wakeup
 * and timeout. In the future, flow engine should immediately retry and then let maestro engine take
 * care of retry delay. It will remove FlowTaskRetry and address those related existing issues.
 *
 * @author jun-he
 */
@Slf4j
final class FlowActor extends BaseActor {
  private static final long JITTER = TimeUnit.SECONDS.toMillis(1);

  private final Flow flow; // read-only access from all child actors
  private final long reconciliationInterval; // flow reconciliation interval in millis
  private final long refreshInterval; // flow refresh (monitor task) interval in millis
  private boolean finalized; // flag indicating if the final call is done

  FlowActor(Flow flow, GroupActor parent, ExecutionContext context) {
    super(context, parent);
    this.flow = flow;
    this.reconciliationInterval = context.getProperties().getFlowReconciliationIntervalInMillis();
    this.refreshInterval = context.getProperties().getFlowRefreshIntervalInMillis();
    this.finalized = false;
  }

  @Override
  void beforeRunning() {
    LOG.info("Start running flow actor for flow: [{}]", reference());
    getMetrics().counter("num_of_running_flows", getClass());
  }

  @Override
  void runForAction(Action action) {
    switch (action) {
      case Action.FlowStart fs -> startFlow(fs);
      case Action.FlowReconcile r -> reconcile(r);
      case Action.FlowRefresh fr -> refresh();
      case Action.FlowTaskRetry t -> retryTask(t.taskRefName());
      case Action.TaskUpdate u -> updateFlow(u.updatedTask());
      case Action.TaskWakeUp w -> wakeup(w.taskRef());
      case Action.FlowTimeout ft -> timeoutFlow();
      case Action.FlowShutdown sd -> startShutdown(Action.TASK_SHUTDOWN);
      case Action.TaskDown td -> checkShutdown();
      default ->
          throw new MaestroUnprocessableEntityException(
              "Unexpected action: [%s] for flow %s", action, reference());
    }
  }

  @Override
  void afterRunning() {
    getMetrics()
        .counter("num_of_finished_flows", getClass(), "finalized", String.valueOf(finalized));
    if (finalized) {
      getContext().deleteFlow(flow);
      LOG.info("Flow for {} is deleted as it finishes.", reference());
    }
  }

  @Override
  String reference() {
    return flow.getReference();
  }

  @Override
  Logger getLogger() {
    return LOG;
  }

  /**
   * If flow start fails, will retry forever. Start flow also takes care of resuming a flow if the
   * flow is relocated to another node as the new owner. In that case, the flow data will be rebuilt
   * by the caller, i.e. maestro engine data in maestro db.
   *
   * @param initAction action to initialize the flow
   */
  private void startFlow(Action.FlowStart initAction) {
    if (initAction.resume()) {
      getContext().resumeFlow(flow);
      if (flow.getStatus().isTerminal()) {
        terminateNow();
        Checks.checkTrue(
            !isRunning(),
            "Expect the flow %s to be terminated but it's not. Retry it",
            reference());
        finalized = true;
        return; // short circuit
      }
      enqueueRetryTasks();
      flow.getRunningTasks().values().forEach(t -> runTask(t, Action.TASK_RESUME));
    } else {
      flow.setPrepareTask(flow.newTask(flow.getFlowDef().getPrepareTask(), true));
      flow.setMonitorTask(flow.newTask(flow.getFlowDef().getMonitorTask(), true));
      getContext().prepare(flow);
    }

    // schedule flow timeout
    var offset = flow.getStartTime() + flow.getTimeoutInMillis() - System.currentTimeMillis();
    schedule(Action.FLOW_TIMEOUT, offset);

    post(new Action.FlowReconcile(System.currentTimeMillis()));
  }

  private void enqueueRetryTasks() {
    Map<String, Task> lastAttemptMap =
        flow.getFinishedTasks().stream()
            .collect(Collectors.toMap(Task::referenceTaskName, t -> t, (first, second) -> second));
    lastAttemptMap.values().forEach(this::scheduleRetryableTask);
  }

  private void reconcile(Action.FlowReconcile action) {
    LOG.debug(
        "Last update at [{}] and no update for [{}]ms, let's reconcile [{}]",
        action.scheduledAt(),
        System.currentTimeMillis() - action.scheduledAt(),
        reference());
    decide();
    schedule(
        new Action.FlowReconcile(System.currentTimeMillis()), delayForNext(reconciliationInterval));
  }

  private long delayForNext(long delayInterval) {
    return delayInterval + (int) (JITTER * Math.random());
  }

  private void refresh() {
    getContext().refresh(flow);
    tryTerminate();
  }

  private void retryTask(String taskRefName) {
    try {
      Task prev =
          flow.getFinishedTasks().stream()
              .filter(t -> Objects.equals(t.referenceTaskName(), taskRefName))
              .reduce((first, second) -> second)
              .orElseThrow(
                  () ->
                      new MaestroNotFoundException(
                          "Cannot retry as no previous task attempt exists for [%s]", taskRefName));

      Task retryTask = flow.newTask(prev.getTaskDef(), false);
      retryTask.setOutputData(prev.getOutputData());
      retryTask.setRetryCount(prev.getRetryCount() + 1);
      runTask(retryTask, Action.TASK_START);
    } catch (RuntimeException e) { // retryTask will not throw exceptions
      LOG.warn("Ignore the exception within retryTask for taskRef [{}] ", taskRefName, e);
    }
  }

  /**
   * Update flow due to any update for this flow, e.g. a task status change, etc.
   *
   * @param updatedTask updated task
   */
  private void updateFlow(Task updatedTask) {
    LOG.debug("Got an updated task: [{}]", updatedTask);
    if (containsChild(updatedTask.referenceTaskName())) {
      flow.updateRunningTask(updatedTask);
      if (!updatedTask.isTerminal()) { // non-critical update
        schedule(Action.FLOW_REFRESH, delayForNext(refreshInterval));
        if (!updatedTask.isActive()) {
          schedule(
              new Action.TaskWakeUp(updatedTask.referenceTaskName()),
              delayForNext(TimeUnit.SECONDS.toMillis(updatedTask.getStartDelayInSeconds())));
        }
      } else {
        scheduleRetryableTask(updatedTask);
        removeChild(updatedTask.referenceTaskName());
        flow.addFinishedTask(updatedTask);
        decide();
      }
    } else {
      LOG.warn(
          "Flow received a update action for task [{}] but it does not exist. Ignore it",
          updatedTask.referenceTaskName());
    }
  }

  private void scheduleRetryableTask(Task task) {
    if (task.getStatus().isRestartable()) {
      long delay = TimeUnit.SECONDS.toMillis(task.getStartDelayInSeconds());
      if (task.getEndTime() == null) {
        LOG.warn(
            "Critical warning for an unexpected case: Flow task [{}][{}] has status [{}] but endTime is unset.",
            reference(),
            task.referenceTaskName(),
            task.getStatus());
      } else {
        delay = Math.max(0, task.getEndTime() + delay - System.currentTimeMillis());
      }
      schedule(new Action.FlowTaskRetry(task.referenceTaskName()), delay);
    }
  }

  // This is the best effort. The task actor might not run while flow thinks it's running or the
  // actor is shutdown. In those cases, missing wakeup will cause the step won't take any action
  // during retry backoff delay. Callers have to retry for wakeup.
  private void wakeup(@Nullable String taskRef) {
    getMetrics()
        .counter("num_of_wakeup_flows", getClass(), "forall", taskRef == null ? "true" : "false");
    if (taskRef == null) { // wakeup all tasks if taskRef is null
      wakeupAll();
      return;
    }
    Task snapshot = flow.getRunningTasks().get(taskRef);
    if (snapshot == null) {
      if (dequeRetryAction(taskRef)) {
        retryTask(taskRef);
      }
    } else if (!snapshot.isActive()) {
      snapshot.setActive(true);
    }
    wakeUpChildActor(taskRef, Action.TASK_ACTIVATE);
  }

  // wake up all tasks but do not activate inactive tasks
  private void wakeupAll() {
    dequeRetryActions().forEach(this::retryTask);
    wakeUpChildActors(Action.TASK_PING);
  }

  private void timeoutFlow() {
    flow.markTimedout();
    dequeRetryActions().forEach(this::retryTask);
    wakeUpChildActors(Action.TASK_STOP);
    schedule(Action.FLOW_TIMEOUT, getRetryInterval()); // retry if flow is not timed out
  }

  private void decide() {
    LOG.debug("Start deciding the whole flow paths in [{}]", reference());
    if (flow.getPrepareTask().getStatus() == Task.Status.FAILED_WITH_TERMINAL_ERROR) {
      // start task fail and then should directly fail the flow
      flow.getMonitorTask().setStatus(Task.Status.CANCELED);
    } else {
      // build a map with step id to the latest task status
      Map<String, Task.Status> taskStatusMap =
          flow.getStreamOfAllTasks()
              .collect(Collectors.toMap(Task::referenceTaskName, Task::getStatus, (u, v) -> v));
      flow.getFlowDef().getTasks().stream()
          .map(t -> nextTask(t, taskStatusMap))
          .filter(Objects::nonNull)
          .forEach(this::startTask);

      getContext().refresh(flow);
    }

    tryTerminate();
  }

  private void tryTerminate() {
    // decide if this flow should be terminated based on the monitor task status
    var status = flow.getMonitorTask().getStatus();
    LOG.info("Monitor task for [{}]'s status is [{}]", reference(), status);
    if (status.isTerminal()) {
      if (status.isSuccessful()) {
        flow.setStatus(Flow.Status.COMPLETED);
      } else {
        flow.setStatus(Flow.Status.FAILED);
      }
      flow.markUpdate();

      try {
        getContext().finalCall(flow);
        finalized = true;
        terminateNow();
        // In certain cases, the flow is decided to reach terminal state while inactive or dummy
        // tasks running, e.g., Maestro engine gate task or task with step status=NOT_CREATED.
        if (flow.getRunningTasks().values().stream().anyMatch(Task::isActive)) {
          LOG.debug(
              "Flow [{}] have active tasks after final callback. Please check it if unexpected",
              flow);
        }
        // Best effort to wake up child actors, so they will finish as parent is not running.
        wakeUpChildActors(Action.TASK_PING);
      } catch (RuntimeException e) {
        LOG.warn(
            "Got an exception during final callback, ignore it and reconciliation will retry ...",
            e);
      }
    }
  }

  private TaskDef nextTask(List<TaskDef> taskList, Map<String, Task.Status> taskStatusMap) {
    ListIterator<TaskDef> iterator = taskList.listIterator(taskList.size());
    while (iterator.hasPrevious()) {
      TaskDef taskDef = iterator.previous();
      var ref = taskDef.taskReferenceName();
      if (taskStatusMap.containsKey(ref)) {
        if (taskStatusMap.get(ref) == Task.Status.COMPLETED) {
          iterator.next(); // skip itself
          return iterator.hasNext() ? iterator.next() : null;
        } else if (joinOnThis(taskDef, taskStatusMap)) {
          getChild(ref).post(Action.TASK_PING);
        }
        return null;
      }
    }
    return taskList.isEmpty() ? null : taskList.getFirst();
  }

  private boolean joinOnThis(TaskDef taskDef, Map<String, Task.Status> taskStatusMap) {
    Task.Status status = taskStatusMap.get(taskDef.taskReferenceName());
    if (taskDef.joinOn() != null
        && !taskDef.joinOn().isEmpty()
        && status == Task.Status.IN_PROGRESS) {
      return taskDef.joinOn().stream()
          .allMatch(ref -> taskStatusMap.get(ref) == Task.Status.COMPLETED);
    }
    return false;
  }

  private void startTask(TaskDef def) {
    Task task = flow.newTask(def, false);
    runTask(task, Action.TASK_START);
  }

  private void runTask(Task task, Action initAction) {
    if (containsChild(task.referenceTaskName())) {
      LOG.error("Unexpected and the task [{}] already exist", task.referenceTaskName());
      throw new MaestroResourceConflictException(
          "Cannot create an already created task for %s", task.referenceTaskName());
    }
    var cloned = getContext().cloneTask(task);
    var actor = new TaskActor(cloned, flow, this, getContext());
    runActionFor(actor, initAction);
  }

  private Stream<String> dequeRetryActions() {
    return getScheduledActions().entrySet().stream()
        .filter(
            e ->
                e.getKey() instanceof Action.FlowTaskRetry
                    && !e.getValue().isDone()
                    && e.getValue().cancel(false))
        .map(e -> ((Action.FlowTaskRetry) e.getKey()).taskRefName());
  }

  private boolean dequeRetryAction(String taskRef) {
    var scheduledActions = getScheduledActions();
    var action = new Action.FlowTaskRetry(taskRef);
    return scheduledActions.containsKey(action)
        && !scheduledActions.get(action).isDone()
        && scheduledActions.get(action).cancel(false);
  }
}
