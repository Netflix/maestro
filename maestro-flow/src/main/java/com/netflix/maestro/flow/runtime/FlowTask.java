package com.netflix.maestro.flow.runtime;

import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;

/**
 * Interface for Maestro Engine to define logics to run a task.
 *
 * @author jun-he
 */
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface FlowTask {
  default void start(Flow flow, Task task) {}

  boolean execute(Flow flow, Task task);

  default void cancel(Flow flow, Task task) {}
}
