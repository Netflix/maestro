package com.netflix.maestro.flow.actor;

import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.FlowGroup;

/**
 * Actor interface to implement flow group actors, flow actors, and task actors.
 *
 * @author jun-he
 */
public sealed interface Actor extends Runnable permits BaseActor {
  boolean isRunning();

  Actor post(Action action);

  long generation();

  long validUntil();

  static Actor startGroupActor(FlowGroup group, ExecutionContext context) {
    var actor = new GroupActor(group, context);
    context.run(actor);
    return actor.post(Action.GROUP_START);
  }
}
