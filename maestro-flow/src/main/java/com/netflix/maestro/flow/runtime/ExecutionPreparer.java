package com.netflix.maestro.flow.runtime;

import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;

/**
 * Execution preparer is used to implement the preparation logic requiring knowing extra details
 * related to the execution engine, i.e. maestro engine.
 *
 * @author jun-he
 */
public interface ExecutionPreparer {
  /**
   * This method provides a way to implement adding extra tasks (i.e. dummy tasks for param) to the
   * flow. It also supports to add some data to the input of a task.
   *
   * @param flow flow to add extra tasks
   * @param task task to add the input
   */
  void addExtraTasksAndInput(Flow flow, Task task);

  /**
   * Make a full copy of the task. It is thread safe.
   *
   * @param task task to clone
   * @return cloned task object
   */
  Task cloneTask(Task task);

  /**
   * Rebuild all transient data for a given flow. It returns a flag to indicate if the prepare task
   * should be called again to finish resuming.
   *
   * @param flow flow to resume
   * @return a flag to indicate if the flow's prepare task should be called
   */
  boolean resume(Flow flow);
}
