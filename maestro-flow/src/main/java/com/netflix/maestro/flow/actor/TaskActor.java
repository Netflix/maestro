package com.netflix.maestro.flow.actor;

import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import java.util.concurrent.TimeUnit;
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

  TaskActor(Task task, Flow flow, FlowActor parent, ExecutionContext context) {
    super(context, parent);
    this.task = task;
    this.flow = flow;
  }

  @Override
  void beforeRunning() {
    getMetrics().counter("num_of_running_tasks", getClass());
  }

  @Override
  void runForAction(Action action) {
    switch (action) {
      case Action.TaskStart s -> start(s.resume());
      case Action.TaskStop ts -> stop();
      case Action.TaskPing p -> execute();
      case Action.TaskTimeout t -> execute();
      case Action.TaskShutdown d -> shutdown();
      default ->
          throw new MaestroUnprocessableEntityException(
              "Unexpected action: [%s] for Task [%s]", action, reference());
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
  Logger getLogger() {
    return LOG;
  }

  private void start(boolean resume) {
    if (!resume) {
      getContext().start(flow, task);
      postTaskUpdate();
    }
    schedule(Action.TASK_PING, TimeUnit.SECONDS.toMillis(task.getStartDelayInSeconds()));
  }

  private void stop() {
    getContext().cancel(flow, task);
    post(Action.TASK_PING);
  }

  /**
   * In NOT_CREATED case, the task actor might run while the parent is not. But the next isRunning
   * check will discover it as it also checks the parent status.
   */
  private void execute() {
    boolean changed = getContext().execute(flow, task);
    if (task.getStatus().isTerminal()) {
      terminateNow(); // if terminal state, then stop
      postTaskUpdate();
    } else {
      // schedule a task timeout once the task is in executed state
      if (task.getStartTime() != null && task.getTimeoutInMillis() != null) {
        var offset =
            Math.max(
                0, task.getStartTime() + task.getTimeoutInMillis() - System.currentTimeMillis());
        schedule(Action.TASK_TIMEOUT, offset);
        task.setTimeoutInMillis(null); // avoid schedule timeout again
      }
      // maestro step directly set startDelay to control the delay interval
      schedule(Action.TASK_PING, TimeUnit.SECONDS.toMillis(task.getStartDelayInSeconds()));

      if (changed) {
        postTaskUpdate();
      }
    }
    task.incrementPollCount();
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
