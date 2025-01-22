package com.netflix.maestro.flow.actor;

import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;

/**
 * Actions supported by flow group actors, flow actors, and task actors.
 *
 * @author jun-he
 */
@SuppressWarnings({"JavadocVariable", "checkstyle:InterfaceIsType"})
public sealed interface Action {
  // actions for flow group actors
  record GroupStart() implements Action {}

  Action GROUP_START = new GroupStart();

  record FlowLaunch(Flow flow, boolean resume) implements Action {}

  record GroupHeartbeat() implements Action {}

  Action GROUP_HEARTBEAT = new GroupHeartbeat();

  record GroupShutdown() implements Action {}

  Action GROUP_SHUTDOWN = new GroupShutdown();

  // FlowDown is only to ack Shutdown when the node is down, not for termination
  record FlowDown() implements Action {}

  Action FLOW_DOWN = new FlowDown();

  record FlowWakeUp(String flowReference, String taskRef) implements Action {}

  // actions for flow actors
  record FlowStart(boolean resume) implements Action {}

  Action FLOW_START = new FlowStart(false);
  Action FLOW_RESUME = new FlowStart(true);

  record FlowReconcile(long scheduledAt) implements Action {}

  record FlowRefresh() implements Action {}

  Action FLOW_REFRESH = new FlowRefresh();

  record FlowTaskRetry(String taskRefName) implements Action {}

  record FlowTimeout() implements Action {}

  Action FLOW_TIMEOUT = new FlowTimeout();

  record FlowShutdown() implements Action {}

  Action FLOW_SHUTDOWN = new FlowShutdown();

  // TaskDown is only to ack Shutdown when the node is down, not for termination
  record TaskDown() implements Action {}

  Action TASK_DOWN = new TaskDown();

  record TaskWakeUp(String taskRef) implements Action {}

  // actions for task actors
  record TaskStart(boolean resume) implements Action {}

  Action TASK_START = new TaskStart(false);
  Action TASK_RESUME = new TaskStart(true);

  record TaskActivate(boolean active) implements Action {}

  Action TASK_ACTIVATE = new TaskActivate(true);

  record TaskStop() implements Action {}

  Action TASK_STOP = new TaskStop();

  // used to wakeup actor, which might be directly scheduled by using TASK_PING constant.
  record TaskPing() implements Action {}

  Action TASK_PING = new TaskPing();

  record TaskUpdate(Task updatedTask) implements Action {}

  record TaskTimeout() implements Action {}

  Action TASK_TIMEOUT = new TaskTimeout();

  record TaskShutdown() implements Action {}

  Action TASK_SHUTDOWN = new TaskShutdown();
}
