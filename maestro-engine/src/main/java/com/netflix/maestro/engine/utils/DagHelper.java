/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/** Utility class for Dag algorithms. */
public final class DagHelper {
  private DagHelper() {}

  /**
   * Check if a DAG with all terminate state steps is actually done.
   *
   * @param runtimeDag the runtime dag with step transitions
   * @param idStatusMap map from step id to a boolean flag, true means the step execution is
   *     succeeded and false means the step execution is failed
   * @return if the DAG is done.
   */
  public static boolean isDone(
      Map<String, StepTransition> runtimeDag,
      Map<String, Boolean> idStatusMap,
      RestartConfig restartConfig) {

    Map<String, Set<String>> parentMap = new HashMap<>();
    Map<String, Set<String>> childMap = new HashMap<>();
    Deque<String> deque =
        prepareDagForTraversal(
            runtimeDag, idStatusMap.keySet(), restartConfig, parentMap, childMap);

    return isDone(idStatusMap, deque, parentMap, childMap);
  }

  /**
   * Go over DAG to build parent and child relationship maps and then trim runtime dag by removing
   * unreachable nodes from it. It returns the deque of root nodes for BFS traversal.
   */
  private static Deque<String> prepareDagForTraversal(
      Map<String, StepTransition> runtimeDag,
      Set<String> knownStepIds,
      RestartConfig restartConfig,
      Map<String, Set<String>> parentMap,
      Map<String, Set<String>> childMap) {
    Deque<String> deque =
        runtimeDag.entrySet().stream()
            .filter(entry -> entry.getValue().getPredecessors().isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toCollection(ArrayDeque::new));

    runtimeDag.forEach(
        (key, val) -> parentMap.put(key, new LinkedHashSet<>(val.getPredecessors())));
    runtimeDag.forEach(
        (key, val) -> childMap.put(key, new LinkedHashSet<>(val.getSuccessors().keySet())));

    // might need to adjust the runtime DAG if restarting from a step
    Optional.ofNullable(restartConfig)
        .flatMap(config -> Optional.ofNullable(RunRequest.getCurrentNode(config).getStepId()))
        .ifPresent(
            stepId -> {
              Set<String> toRemove = new HashSet<>(deque);
              Checks.checkTrue(
                  toRemove.remove(stepId),
                  "Invalid state: stepId [%s] should be one of start nodes in the DAG",
                  stepId);
              toRemove.removeIf(knownStepIds::contains);
              deque.removeAll(toRemove);
              trimDag(knownStepIds, new ArrayDeque<>(toRemove), parentMap, childMap);
            });
    return deque;
  }

  /**
   * BFS to remove nodes from the DAG reachable from all step ids within the input queue. It mutates
   * the key space and values of childMap and mutates only values of parentMap.
   */
  private static void trimDag(
      Set<String> idStatusMap,
      Deque<String> deque,
      Map<String, Set<String>> parentMap,
      Map<String, Set<String>> childMap) {
    Set<String> visited = new HashSet<>();
    while (!deque.isEmpty()) {
      String stepIdToRemove = deque.poll();
      Checks.checkTrue(
          !idStatusMap.contains(stepIdToRemove),
          "Invalid state: stepId [%s] should not have any status",
          stepIdToRemove);
      parentMap.get(stepIdToRemove).forEach(parent -> childMap.get(parent).remove(stepIdToRemove));
      childMap.get(stepIdToRemove).forEach(child -> parentMap.get(child).remove(stepIdToRemove));
      Set<String> toAdd =
          childMap.remove(stepIdToRemove).stream()
              .filter(id -> !visited.contains(id))
              .collect(Collectors.toSet());
      deque.addAll(toAdd);
      visited.addAll(toAdd);
    }
  }

  /**
   * Topological traversal over the whole DAG defined by parentMap and childMap to check if all
   * reachable nodes in the DAG are in terminal states. It mutates the key space and values of
   * idStatusMap and childMap and mutate only values of parentMap.
   */
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  private static boolean isDone(
      Map<String, Boolean> idStatusMap,
      Deque<String> deque,
      Map<String, Set<String>> parentMap,
      Map<String, Set<String>> childMap) {
    while (!deque.isEmpty()) {
      String stepId = deque.poll();
      Boolean status = idStatusMap.remove(stepId);
      if (status == null) {
        return false;
      } else if (status) {
        childMap
            .remove(stepId)
            .forEach(
                nextStepId -> {
                  parentMap.get(nextStepId).remove(stepId);
                  if (parentMap.get(nextStepId).isEmpty()) {
                    deque.offer(nextStepId);
                  }
                });
      } else {
        trimDag(
            idStatusMap.keySet(),
            new ArrayDeque<>(Collections.singletonList(stepId)),
            parentMap,
            childMap);
      }
    }
    Checks.checkTrue(
        idStatusMap.isEmpty(),
        "Invalid state: steps [%s] are still within status map",
        idStatusMap.keySet());
    Checks.checkTrue(
        childMap.isEmpty(), "Invalid state: steps [%s] are still within child map", childMap);
    return true;
  }

  /**
   * It computes the stepIds included/reachable in the current runtime DAG of the workflow instance
   * run.
   */
  public static Set<String> computeStepIdsInRuntimeDag(
      @NotNull WorkflowInstance instance, @NotNull Set<String> knownStepIds) {
    if (instance.isFreshRun()
        || instance.getRunConfig() == null
        || instance.getRunConfig().getPolicy() != RunPolicy.RESTART_FROM_SPECIFIC) {
      return instance.getRuntimeDag().keySet();
    }

    Map<String, Set<String>> childMap = new HashMap<>();
    prepareDagForTraversal(
        instance.getRuntimeDag(),
        knownStepIds,
        instance.getRunConfig().getRestartConfig(),
        new HashMap<>(),
        childMap);

    return childMap.keySet();
  }

  /**
   * Get the root nodes to be NOT_CREATED in the runtime dag when restarting. This is useful only
   * for RunPolicy.RESTART_FROM_SPECIFIC.
   */
  public static Set<String> getNotCreatedRootNodesInRestartRuntimeDag(
      @NotNull Map<String, StepTransition> runtimeDag, @NotNull RestartConfig restartConfig) {
    Set<String> rootStepIds =
        runtimeDag.entrySet().stream()
            .filter(entry -> entry.getValue().getPredecessors().isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    Optional.ofNullable(RunRequest.getCurrentNode(restartConfig).getStepId())
        .ifPresent(
            stepId ->
                Checks.checkTrue(
                    rootStepIds.remove(stepId),
                    "Invalid state: stepId [%s] should be one of root nodes in the DAG",
                    stepId));
    return rootStepIds;
  }
}
