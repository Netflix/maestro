package com.netflix.maestro.flow.actor;

import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.Constants;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Task actor is responsible to run through task's underlying business logic, which is the maestro
 * step. It is highly optimized for maestro engine, although it is loosely coupled with maestro
 * engine. All the states of task actors are in memory and safe to be lost without any issue. The
 * maestro engine is able to rebuild the task states if lost. For tasks running underlying Maestro
 * subworkflow or foreach workflow, it can co-locate those nested maestro workflow instances in the
 * same node to achieve the best performance. But this is not required, and they can be located in
 * other nodes. Additionally, Task actors will pass a small portion of necessary data back to the
 * flow for sharing with all other running task actors.
 *
 * @author jun-he
 */
@Slf4j
final class TaskActor extends BaseActor {
  private final Task task;
  private final Flow flow;
  private final String name;

  TaskActor(Task task, Flow flow, FlowActor parent, ExecutionContext context) {
    super(context, parent);
    this.task = task;
    this.flow = flow;
    this.name = flow.getReference() + "[" + task.referenceTaskName() + "]";
  }

  @Override
  void beforeRunning() {
    LOG.info("Start running task actor for flow task: [{}]", name);
    getMetrics().counter("num_of_running_tasks", getClass());
  }

  @Override
  void runForAction(Action action) {
    switch (action) {
      case Action.TaskStart s -> start(s.resume());
      case Action.TaskStop ts -> stop();
      case Action.TaskPing p -> execute(p.code());
      case Action.TaskActivate a -> activate(a.code());
      case Action.TaskTimeout t -> execute(Constants.TIMEOUT_TASK_CODE);
      case Action.TaskShutdown d -> shutdown();
      default ->
          throw new MaestroUnprocessableEntityException(
              "Unexpected action: [%s] for Task [%s]", action, name);
    }
  }

  @Override
  void afterRunning() {
    getMetrics().counter("num_of_finished_tasks", getClass());
  }

  @Override
  String reference() {
    return task.referenceTaskName();
  }

  @Override
  String name() {
    return name;
  }

  @Override
  Logger getLogger() {
    return LOG;
  }

  // start will initialize any tasks, including active and inactive
  private void start(boolean resume) {
    if (!resume) {
      getContext().start(flow, task);
    }
    post(Action.TASK_PING); // immediately execute over TASK_PING
    task.setStarted(true);
  }

  private void activate(int code) {
    if (!task.isActive()) {
      task.setActive(true);
    }
    // cancel any existing scheduled task ping as the activate call does the same action.
    var dedupAction =
        code == Constants.TASK_PING_CODE ? Action.TASK_PING : new Action.TaskPing(code);
    var future = getScheduledActions().get(dedupAction);
    if (future != null) {
      future.cancel(false);
    }
    execute(code);
  }

  private void stop() {
    if (task.isActive()) {
      getContext().cancel(flow, task);
      if (task.getStatus().isTerminal()) {
        terminateNow(); // if terminal state, then stop
        postTaskUpdate();
      } else {
        schedule(Action.TASK_STOP, getRetryInterval()); // schedule a retry
      }
    }
  }

  /**
   * In NOT_CREATED case, the task actor might run while the parent is not. But the next isRunning
   * check will discover it as it also checks the parent status. Maestro engine makes sure to flip
   * the active flag if the task should not execution (e.g. NOT_CREATED case).
   */
  private void execute(int code) {
    if (!task.isStarted()) {
      LOG.info("Flow task [{}] is not started yet, skip execution", name);
      return;
    }
    boolean changed = false;
    if (task.isActive()) { // execution only for active tasks
      task.setCode(code);
      changed = getContext().execute(flow, task);
      task.setCode(Constants.TASK_PING_CODE);
    }
    if (task.getStatus().isTerminal()) {
      terminateNow(); // if terminal state, then stop
      changed = true;
    } else if (task.isActive()) {
      // schedule a task timeout once the task is in executed state
      if (task.getStartTime() != null && task.getTimeoutInMillis() != null) {
        var offset =
            Math.max(
                0, task.getStartTime() + task.getTimeoutInMillis() - System.currentTimeMillis());
        schedule(Action.TASK_TIMEOUT, offset);
        task.setTimeoutInMillis(null); // avoid schedule timeout again
      }
      task.incrementPollCount();
    }

    if (changed) {
      postTaskUpdate();
    }

    if (isRunning() && task.isActive()) { // if inactive, rely on flow to periodically wakeup
      // maestro step directly set startDelay to control the delay interval
      schedule(Action.TASK_PING, task.getStartDelayInMillis());
    }
  }

  private void postTaskUpdate() {
    var cloned = getContext().cloneTask(task);
    getParent().post(new Action.TaskUpdate(cloned));
  }

  private void shutdown() {
    terminateNow();
    getParent().post(Action.TASK_DOWN);
  }
}
