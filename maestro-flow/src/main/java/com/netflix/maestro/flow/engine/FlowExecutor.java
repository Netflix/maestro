package com.netflix.maestro.flow.engine;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.flow.Constants;
import com.netflix.maestro.flow.actor.Action;
import com.netflix.maestro.flow.actor.Actor;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.utils.ExecutionHelper;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

/**
 * Flow executor is the public class for the callers to interact with flow engine. It also claims
 * ownership of an expired flow group and submit flow to its flow groups, such as creating a flow
 * for a flow groups, or removing a flow group if flow group is not running (e.g. losing
 * ownership),etc. It also manages to start or shutdown a flow group. It is expected to be a
 * singleton and can be safely called by multiple threads.
 *
 * @author jun-he
 */
@Slf4j
public class FlowExecutor {
  private final ScheduledExecutorService maintainer = Executors.newSingleThreadScheduledExecutor();
  private final Map<Long, Actor> groupActors = new ConcurrentHashMap<>(); // never remove
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();

  private final ExecutionContext context;
  private final long initialDelay;
  private final long delay;
  private final long maxGroupNumPerNode;
  private final String address;

  /** Constructor. */
  public FlowExecutor(ExecutionContext context) {
    this.context = context;
    this.initialDelay = context.getProperties().getInitialMaintenanceDelayInMillis();
    this.delay = context.getProperties().getMaintenanceDelayInMillis();
    this.maxGroupNumPerNode = context.getProperties().getMaxGroupNumPerNode();
    this.address = context.getProperties().getEngineAddress();
  }

  /** PostConstructor to boostrap and start flow executor. */
  public void init() {
    LOG.info(
        "FlowExecutor is initialized with initial_delay: {}, delay_interval: {} and address {}",
        initialDelay,
        delay,
        address);
    maintainer.scheduleWithFixedDelay(
        this::maintenance, initialDelay, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * This method periodically get non-heartbeat flow groups from the db and claim it. Each node is
   * expected to manage a reasonable number (e.g. less than 10) of groups. No need to create many
   * groups as a group can include as many as running maestro flow instances. Group helps spread the
   * load into the whole cluster with minimal maintenance cost, e.g. heartbeat. We should set
   * max_group_num to be reasonable size and always <= max_group_num_per_node * number of nodes.
   * When we need to add more nodes, It's better to do a new fresh deployment to replace the whole
   * cluster with a larger size rather than adding a few nodes into the existing cluster. In a
   * cluster setup (>=3 nodes), a usual simple way is to set max_group_num_per_node to be 5 and
   * max_group_num to be 3 * num of nodes. e.g. 3 nodes, then max_group_num = 9 and
   * max_group_num_per_node = 5. If one node is dead, other two can still pick those groups. This
   * support also supports autoscaling upto 3x to 9 nodes. Also note that those configurations can
   * be inconsistent across the cluster. Adjusting max_group_num and max_group_num_per_node won't
   * affect existing flow execution as the group id has been decided when the instance is created.
   * It only affects the new flow instances. Existing flows will continue to work as they are
   * assigned a decided group id at the beginning.
   *
   * <p>In the future, we might add support for nodes to talk with each other to take the ownership
   * a group from existing nodes.
   */
  private void maintenance() {
    LOG.trace("[{}] tries to claim a group...", address);
    int groupNum = groupActors.size();
    context.getMetrics().gauge("num_of_groups", groupNum, getClass());
    if (groupNum >= maxGroupNumPerNode) {
      LOG.trace(
          "[{}] has enough groups [{} >= {}], no need to claim more",
          address,
          groupNum,
          maxGroupNumPerNode);
      return;
    }
    try {
      FlowGroup group = context.claimGroup();
      if (group != null) {
        LOG.info("[{}] claimed a group [{}]", address, group);
        try {
          readLock.lock();
          groupActors.computeIfAbsent(group.groupId(), id -> Actor.startGroupActor(group, context));
        } finally {
          readLock.unlock();
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("[{}] failed to claim a group due to an error, will try again", address, e);
    }
  }

  /** Pre-destroy to shut down flow executors. */
  public void shutdown() {
    LOG.info("FlowExecutor starts to shutdown ...");
    try {
      writeLock.lock(); // make sure no new flow launching
      groupActors.values().forEach(actor -> actor.post(Action.GROUP_SHUTDOWN));
      context.shutdown();
      ExecutionHelper.shutdown(maintainer, "FlowExecutor maintainer");
    } finally {
      writeLock.unlock();
    }
    LOG.info("FlowExecutor shutdown is completed.");
  }

  /**
   * Start a maestro flow. It might be allocated to a dead actor but maintenance will release it.
   *
   * @param groupId group id to group flow instances to achieve locality
   * @param flowId the flow id, which must be unique, e.g. a random uuid
   * @param reference reference is what the caller would like to refer a flow
   * @param flowDef flow definition with flow structure and task def, etc.
   * @param flowInput flow input from the caller
   * @return the flow identifier
   * @throws com.netflix.maestro.exceptions.MaestroRetryableError for the caller to retry
   */
  public String startFlow(
      long groupId,
      String flowId,
      String reference,
      FlowDef flowDef,
      Map<String, Object> flowInput) {
    Actor groupActor = getOrCreateNewGroup(groupId);

    // build a maestro flow
    long curTime = System.currentTimeMillis();
    Flow flow = new Flow(groupId, flowId, groupActor.generation(), curTime, reference);
    flow.setInput(flowInput);
    flow.setFlowDef(flowDef);
    flow.setStatus(Flow.Status.RUNNING);
    flow.setUpdateTime(curTime);

    // add the flow into a group and persist it
    context.saveFlow(flow);
    groupActor.post(new Action.FlowLaunch(flow, false));
    return flow.getFlowId();
  }

  /**
   * If the group id exists in the node, it uses the existing live actor. Otherwise, it tries to
   * create a new one, which might fail if the group already exists. In that case, it throws a
   * retryable error to tell the caller to retry. The caller should then find the right node with
   * the group ownership to start the flow. If it creates one successfully, it will assign the flow
   * to this group and ask the group actor launch the flow.
   *
   * @param groupId group id for the flow group
   * @return the flow group actor
   */
  private Actor getOrCreateNewGroup(long groupId) {
    FlowGroup group = new FlowGroup(groupId, Constants.INITIAL_GENERATION_NUMBER, address);
    try {
      readLock.lock();
      return groupActors.computeIfAbsent(
          group.groupId(),
          id -> {
            context.trySaveGroup(group);
            return Actor.startGroupActor(group, context);
          });
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Wake up a flow or a task.
   *
   * @param groupId group id to group flow instances
   * @param flowReference flow reference
   * @param taskReference task reference. If it is null, it wakes up all the tasks in the flow.
   * @return true if the flow or task is woken up successfully, otherwise, false. The caller can
   *     retry based on the returned result.
   */
  public boolean wakeUp(long groupId, String flowReference, @Nullable String taskReference) {
    Actor groupActor = groupActors.get(groupId);
    if (groupActor != null && groupActor.isRunning()) {
      groupActor.post(new Action.FlowWakeUp(flowReference, taskReference));
      return true;
    }
    return false;
  }
}
