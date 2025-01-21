package com.netflix.maestro.flow.actor;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.metrics.MaestroMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Stream;
import lombok.Getter;
import org.slf4j.Logger;

/**
 * Base actor class to provide basic methods shared by all actors.
 *
 * @author jun-he
 */
abstract sealed class BaseActor implements Actor permits GroupActor, FlowActor, TaskActor {
  private final BlockingQueue<Action> actions = new LinkedBlockingQueue<>();
  // it may contain finished actions, which will be replaced during schedule() call
  private final Map<Action, ScheduledFuture<?>> scheduledActions = new HashMap<>();
  private final Map<String, BaseActor> childActors = new HashMap<>();

  private volatile boolean running = true;
  @Getter private final ExecutionContext context; // shared execution context for all actors
  @Nullable @Getter private final Actor parent; // if null, then it is the root actor
  @Getter private final long retryInterval; // action failure retry interval in millis

  BaseActor(ExecutionContext context, @Nullable Actor parent) {
    this.context = context;
    this.parent = parent;
    this.retryInterval = context.getProperties().getActorErrorRetryIntervalInMillis();
  }

  @Override
  public boolean isRunning() {
    if (parent == null) {
      return running;
    }
    return running && parent.isRunning();
  }

  @Override
  public BaseActor post(Action action) {
    actions.offer(action);
    return this;
  }

  @Override
  public long generation() {
    if (parent != null) {
      return parent.generation();
    }
    throw new UnsupportedOperationException(
        getClass().getSimpleName()
            + " does not support getting generation info for "
            + reference());
  }

  abstract void beforeRunning();

  abstract void runForAction(Action action);

  abstract void afterRunning();

  abstract String reference();

  abstract Logger getLogger();

  MaestroMetrics getMetrics() {
    return context.getMetrics();
  }

  void runActionFor(BaseActor actor, Action action) {
    try {
      context.run(actor);
      actor.post(action);
      childActors.put(actor.reference(), actor);
    } catch (RejectedExecutionException re) {
      getLogger()
          .warn(
              "The whole Maestro flow engine is shutdown and cannot take action [{}] for [{}], Ignore the action.",
              action,
              actor.reference());
    }
  }

  void wakeUpChildActors(Action action) {
    childActors.forEach((ref, actor) -> actor.post(action));
  }

  void wakeUpChildActor(String ref, Action action) {
    BaseActor actor = childActors.get(ref);
    if (actor != null && actor.isRunning()) {
      actor.post(action);
    }
  }

  boolean containsChild(String ref) {
    return childActors.containsKey(ref);
  }

  BaseActor getChild(String ref) {
    return childActors.get(ref);
  }

  BaseActor removeChild(String ref) {
    return childActors.remove(ref);
  }

  void cleanupChildActors() {
    childActors.entrySet().removeIf(entry -> !entry.getValue().isRunning());
  }

  boolean noChildActorsRunning() {
    return childActors.values().stream().noneMatch(BaseActor::isRunning);
  }

  void startShutdown(Action action) {
    if (childActors.isEmpty()) {
      checkShutdown();
    } else {
      cancelPendingActions();
      wakeUpChildActors(action);
    }
  }

  void checkShutdown() {
    if (noChildActorsRunning()) {
      terminateNow();
      if (parent != null) {
        parent.post(Action.FLOW_DOWN);
      }
    }
  }

  void terminateNow() {
    cancelPendingActions();
    running = false;
  }

  void cancelPendingActions() {
    scheduledActions.values().forEach(f -> f.cancel(true));
  }

  Stream<String> dequeRetryActions() {
    return scheduledActions.entrySet().stream()
        .filter(
            e ->
                e.getKey() instanceof Action.FlowTaskRetry
                    && !e.getValue().isDone()
                    && e.getValue().cancel(false))
        .map(e -> ((Action.FlowTaskRetry) e.getKey()).taskRefName());
  }

  boolean dequeRetryAction(String taskRef) {
    var action = new Action.FlowTaskRetry(taskRef);
    return scheduledActions.containsKey(action)
        && !scheduledActions.get(action).isDone()
        && scheduledActions.get(action).cancel(false);
  }

  void schedule(Action action, long delayInMillis) {
    if (!isRunning()
        || (scheduledActions.containsKey(action) && !scheduledActions.get(action).isDone())) {
      getLogger()
          .debug(
              "skip posting action [{}] as either it's not running or there is already one for [{}]",
              action,
              reference());
    } else if (delayInMillis <= 0) {
      post(action);
    } else {
      getLogger()
          .debug(
              "enqueue an action [{}] for [{}] with a delay [{}]ms",
              action,
              reference(),
              delayInMillis);
      var future = context.schedule(() -> actions.offer(action), delayInMillis);
      scheduledActions.put(action, future);
    }
  }

  @Override
  public void run() {
    getLogger().info("[{}] is ready to run", reference());
    beforeRunning();
    getLogger().debug("[{}] is running", reference());
    while (isRunning()) {
      Action action = dequeueAction();
      if (action == null) { // interrupted
        continue;
      }
      try {
        if (isRunning()) { // double check to avoid extra run but no guarantee
          runForAction(action);
        }
      } catch (RuntimeException e) {
        getLogger()
            .warn(
                "[{}] got an exception for action [{}] and will retry the action",
                reference(),
                action,
                e);
        schedule(action, retryInterval); // requeue and retry the action after some interval
      }
    }
    getLogger().debug("[{}] is not running any more.", reference());
    afterRunning();
    getLogger().info("[{}] is done after running.", reference());
  }

  private Action dequeueAction() {
    try {
      Action action = actions.take();
      getLogger().debug("dequeued an action [{}] for [{}]", action, reference());
      return action;
    } catch (InterruptedException e) {
      getLogger()
          .warn(
              "[{}] is interrupted, running flag value for [{}] is [{}]",
              Thread.currentThread(),
              reference(),
              running);
      terminateNow();
      return null;
    }
  }
}
