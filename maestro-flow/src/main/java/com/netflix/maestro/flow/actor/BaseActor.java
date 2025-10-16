package com.netflix.maestro.flow.actor;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.metrics.MaestroMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import lombok.Getter;
import org.slf4j.Logger;

/**
 * Base actor class to provide basic methods shared by all actors.
 *
 * @author jun-he
 */
abstract sealed class BaseActor implements Actor permits GroupActor, FlowActor, TaskActor {
  private final BlockingQueue<Action> actions = new LinkedBlockingQueue<>();
  // best effort de-duplication. Still possible that same actions are in the queue multiple times.
  private final Map<Action, Boolean> queuedActions = new ConcurrentHashMap<>();
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
    queuedActions.computeIfAbsent(
        action,
        a -> {
          actions.offer(a);
          return Boolean.TRUE;
        });
    return this;
  }

  @Override
  public long generation() {
    if (parent != null) {
      return parent.generation();
    }
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support getting generation info for " + name());
  }

  @Override
  public long validUntil() {
    if (parent != null) {
      return parent.validUntil();
    }
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support getting validUntil info for " + name());
  }

  /** best effort operations without retry support. */
  abstract void beforeRunning();

  abstract void runForAction(Action action);

  /** best effort operations without retry support. */
  abstract void afterRunning();

  /** unique identifier as the actor reference. */
  abstract String reference();

  /** log friendly name for logging. */
  String name() {
    return reference();
  }

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
              actor.name());
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
    if (!checkShutdown()) {
      cancelPendingActions();
      wakeUpChildActors(action);
    }
  }

  // return true if shutdown is finished, otherwise false
  boolean checkShutdown() {
    if (noChildActorsRunning()) {
      terminateNow();
      if (parent != null) {
        parent.post(Action.FLOW_DOWN);
      }
      return true;
    }
    return false;
  }

  void terminateNow() {
    cancelPendingActions();
    running = false;
  }

  private void cancelPendingActions() {
    scheduledActions.values().forEach(f -> f.cancel(true));
  }

  void schedule(Action action, long delayInMillis) {
    if (delayInMillis <= 0) {
      post(action);
    } else if (!isRunning()
        || (scheduledActions.containsKey(action) && !scheduledActions.get(action).isDone())) {
      getLogger()
          .debug(
              "skip posting action [{}] as either it's not running or there is already one for [{}]",
              action,
              name());
    } else {
      getLogger()
          .debug(
              "enqueue an action [{}] for [{}] with a delay [{}]ms", action, name(), delayInMillis);
      var future = context.schedule(() -> post(action), delayInMillis);
      scheduledActions.put(action, future);
    }
  }

  @Override
  public void run() {
    getLogger().info("[{}] is ready to run", name());
    beforeRunning();
    getLogger().debug("[{}] is running", name());
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
                name(),
                action,
                e);
        schedule(action, retryInterval); // requeue and retry the action after some interval
      }
    }
    getLogger().debug("[{}] is not running any more.", name());
    afterRunning();
    getLogger().info("[{}] is done after running.", name());
  }

  private Action dequeueAction() {
    try {
      Action action = actions.take();
      queuedActions.remove(action);
      getLogger().debug("dequeued an action [{}] for [{}]", action, name());
      return action;
    } catch (InterruptedException e) {
      getLogger()
          .warn(
              "[{}] is interrupted, running flag value for [{}] is [{}]",
              Thread.currentThread(),
              name(),
              running);
      terminateNow();
      return null;
    }
  }

  @VisibleForTesting
  Map<Action, ScheduledFuture<?>> getScheduledActions() {
    return scheduledActions;
  }

  @VisibleForTesting
  BlockingQueue<Action> getActions() {
    return actions;
  }

  @VisibleForTesting
  Map<Action, Boolean> getQueuedActions() {
    return queuedActions;
  }
}
