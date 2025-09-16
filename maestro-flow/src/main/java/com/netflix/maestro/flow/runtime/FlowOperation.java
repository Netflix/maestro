package com.netflix.maestro.flow.runtime;

import com.netflix.maestro.flow.Constants;
import com.netflix.maestro.flow.models.FlowDef;
import java.util.Map;
import java.util.Set;

/**
 * This is the interface for flow operations. It includes start a flow or wake up flows in the
 * distributed flow engine cluster. If the flow engine is running in multiple nodes, the operation
 * implementation must be able to reach other nodes. Flow engine provides the necessary data model
 * support (e.g. address field in FlowGroup class) to be used by the operation implementation. The
 * FlowOperation interface can be implemented based on multiple technologies, such as socket, RPC,
 * REST, Zookeeper, distributed job queue, database, etc.
 *
 * <p>For socket, RPC, or REST implementation, it can send the synchronous API request to the target
 * node and get the response from the remote node. This requires to know the target node address,
 * which is the part of flow group data model.
 *
 * <p>The distributed job queue requires to dispatch the request to the worker with the ownership of
 * the flow group. Most queueing systems does not support it. Instead, we can broadcast the request
 * to all the nodes and then the worker with the group ownership will take the operation and other
 * nodes discard it.
 *
 * <p>For database implementation, it can simply save requests to a database table and then the
 * worker (i.e. GroupActor) can query the database table periodically to get the requests and then
 * delete them after processing.
 *
 * @author jun-he
 */
public interface FlowOperation {
  /**
   * Start a maestro flow.
   *
   * @param groupId group id to group flow instances
   * @param flowId the flow id, which must be unique, e.g. a random uuid
   * @param flowReference reference is what the caller would like to refer a flow
   * @param flowDef flow definition with flow structure and task def, etc.
   * @param flowInput flow input from the caller
   * @return the flow identifier
   * @throws com.netflix.maestro.exceptions.MaestroRetryableError for the caller to retry
   */
  String startFlow(
      long groupId,
      String flowId,
      String flowReference,
      FlowDef flowDef,
      Map<String, Object> flowInput);

  /**
   * Wake up a single task in a flow for a group.
   *
   * @param groupId group id to group flow instances
   * @param flowReference reference is what the caller would like to refer a flow
   * @param taskReference task reference
   * @param code notification signaling code, which is passed to the task when it is woken up
   * @return true if the task is woken up successfully. Otherwise, false. The caller can retry based
   *     on the returned result.
   */
  boolean wakeUp(long groupId, String flowReference, String taskReference, int code);

  default boolean wakeUp(long groupId, String flowReference, String taskReference) {
    return wakeUp(groupId, flowReference, taskReference, Constants.TASK_PING_CODE);
  }

  /**
   * Wake up all the tasks in a list of flows for a group.
   *
   * @param groupId group id to group flow instances
   * @param flowReferences flow references
   * @param code notification signaling code, which is passed to the task when it is woken up
   * @return true if all the flows are woken up successfully. Otherwise, false. The caller can retry
   *     based on the returned result.
   */
  boolean wakeUp(long groupId, Set<String> flowReferences, int code);

  default boolean wakeUp(long groupId, Set<String> flowReferences) {
    return wakeUp(groupId, flowReferences, Constants.TASK_PING_CODE);
  }
}
