package com.netflix.maestro.flow.actor;

import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.engine.ExecutionContext;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowGroup;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * It manages a group of flows. The goal is to achieve the locality and reduce DB read and write for
 * flows. A group of flows will heartbeat as a group and be assigned to a single host. Group id is
 * decided by the caller (i.e. maestro engine) to make sure the data locality. Group size
 * configurations should be carefully tuned to not too large but also not too small. It handles
 * heartbeat, launch flow, resume flow, release ownership, and shutdown. NOte that claiming
 * ownership has to happen outside (i.e. in the flow executor) before creating this flow group
 * actor. Also, resuming a flow to rebuild the flow from maestro data is done by the corresponding
 * flow actor.
 *
 * <p>Future work: 1. when heartbeat we can also add a cleanup here to delete group if it is larger
 * than max_group plus idle for quite a while; 2. when exiting, if group id is bigger than max_group
 * number and also no flows in that group, we can delete this group from the table.
 *
 * @author jun-he
 */
@Slf4j
final class GroupActor extends BaseActor {
  private static final String FLOW_ID_START_CURSOR = "";
  private final FlowGroup group;
  private final long heartbeatInterval;
  private final long fetchLimit;
  private volatile long validUntil;

  GroupActor(FlowGroup group, ExecutionContext context) {
    super(context, null);
    this.group = group;
    this.heartbeatInterval = context.getProperties().getHeartbeatIntervalInMillis();
    this.fetchLimit = context.getProperties().getGroupFlowFetchLimit();
    this.validUntil = group.heartbeatTs() + context.getProperties().getExpirationDurationInMillis();
  }

  @Override
  public long generation() {
    return group.generation();
  }

  @Override
  public long validUntil() {
    return validUntil;
  }

  @Override
  void beforeRunning() {
    LOG.info("Start running group actor for group: [{}]", group);
    getMetrics().counter("num_of_running_groups", getClass());
  }

  @Override
  void runForAction(Action action) {
    switch (action) {
      case Action.GroupStart g -> startGroup();
      case Action.FlowLaunch l -> runFlow(l);
      case Action.FlowWakeUp w ->
          wakeUpChildActor(w.flowReference(), new Action.TaskWakeUp(w.taskRef()));
      case Action.GroupHeartbeat h -> heartbeat();
      case Action.GroupShutdown s -> startShutdown(Action.FLOW_SHUTDOWN);
      case Action.FlowDown d -> checkShutdown();
      default ->
          throw new MaestroUnprocessableEntityException(
              "Unexpected action: [%s] for FlowGroup [%s]", action, reference());
    }
  }

  /** The group ownership is released after group actor finishes running. */
  @Override
  void afterRunning() {
    getContext().releaseGroup(group);
    getMetrics().counter("num_of_finished_groups", getClass());
  }

  @Override
  String reference() {
    return "group-" + group.groupId();
  }

  @Override
  Logger getLogger() {
    return LOG;
  }

  /**
   * During start a flow group, it fetches all the existing flows in the flow table over a flow_id
   * cursor. During fetching, it also updates the generation too. If there is an error, it will
   * start it again. With a generation filter, it can skip already loaded flows. Note that a flow
   * might be loaded by more than 1 node during race condition. Generation ensures the current owner
   * will load every flow. Old owners will detect it during heartbeat or DB insert.
   */
  private void startGroup() {
    schedule(Action.GROUP_HEARTBEAT, heartbeatInterval);
    String idCursor = FLOW_ID_START_CURSOR;
    int cnt = 0;
    while (idCursor != null) {
      List<Flow> flows = getContext().getFlowsFrom(group, fetchLimit, idCursor);
      if (flows == null) { // indicate an exception
        schedule(Action.GROUP_START, getRetryInterval()); // schedule a retry
        idCursor = null;
      } else {
        LOG.debug("Loaded [{}] flows for group [{}]", flows.size(), group);
        cnt += flows.size();
        idCursor =
            flows.stream()
                .map(
                    f -> {
                      post(new Action.FlowLaunch(f, true)); // flow is only partially filled
                      return f.getFlowId();
                    })
                .max(String::compareTo)
                .orElse(null);
      }
    }
    // If this takes longer than expiration interval, need to start in phases.
    LOG.info("Finished group start and loaded [{}] flows for the group [{}]", cnt, group);
  }

  private void runFlow(Action.FlowLaunch action) {
    Flow flow = action.flow();
    if (containsChild(flow.getReference())) {
      LOG.error("Unexpected and the flow [{}] already exists, ignoring it", flow.getReference());
      return;
    }
    var actor = new FlowActor(flow, this, getContext());
    runActionFor(actor, action.resume() ? Action.FLOW_RESUME : Action.FLOW_START);
  }

  private void heartbeat() {
    LOG.debug("Heartbeat the group: [{}]", group);
    getMetrics().gauge("current_running_groups", 1.0, getClass(), "group", reference());
    cleanupChildActors(); // cleanup finished flows
    validUntil = getContext().heartbeatGroup(group);
    schedule(Action.GROUP_HEARTBEAT, heartbeatInterval);
  }
}
