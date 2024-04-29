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
package com.netflix.maestro.engine.transformation;

import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Workflow graph representing the steps and dag defined in a workflow definition. */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
final class WorkflowGraph {

  private static final class GraphNode {
    private final Map<String, GraphNode> parents = new LinkedHashMap<>();
    private final Map<String, GraphNode> children = new LinkedHashMap<>();
    private final String stepId;

    private GraphNode(String stepId) {
      this.stepId = Checks.notNull(stepId, "Step ID in the DAG cannot be null");
    }

    private void addChild(GraphNode node) {
      this.children.put(node.stepId, node);
    }

    private void addParent(GraphNode node) {
      this.parents.put(node.stepId, node);
    }
  }

  private final Set<GraphNode> startNodes;
  @Getter private final List<String> endNodeIds;
  private final Map<String, Step> stepMap;

  /**
   * Get all paths.
   *
   * @param translator translator from step to type T.
   * @param <T> path node type.
   * @return all paths.
   */
  <T> List<List<T>> computePaths(Translator<Step, T> translator) {
    List<List<T>> taskPaths = new ArrayList<>();
    while (!startNodes.isEmpty()) {
      Iterator<GraphNode> iterator = startNodes.iterator();
      GraphNode start = iterator.next();
      iterator.remove();
      List<T> path = getPath(start, translator);
      taskPaths.add(path);
    }
    return taskPaths;
  }

  @SuppressWarnings({"PMD.AvoidReassigningLoopVariables"})
  private <T> List<T> getPath(GraphNode start, Translator<Step, T> translator) {
    List<T> path = new ArrayList<>();
    Set<GraphNode> pendingStarts = new LinkedHashSet<>();
    Set<GraphNode> nodesOnPath = new HashSet<>();

    for (GraphNode cur = start; cur != null; ) {
      GraphNode next = null;
      if (!cur.children.isEmpty()) {
        next = cur.children.values().iterator().next(); // pick the first one
        cur.children.remove(next.stepId);
        next.parents.remove(cur.stepId);
        pendingStarts.addAll(cur.children.values());
      }
      if (!cur.parents.isEmpty()) {
        path.add(translator.translate(JoinStep.of(cur.stepId, getJoinStepIds(cur.stepId))));
        for (GraphNode node : cur.parents.values()) {
          node.children.remove(cur.stepId);
        }
      }
      path.add(translator.translate(stepMap.get(cur.stepId)));
      nodesOnPath.add(cur);
      cur = next;
    }
    pendingStarts.removeAll(nodesOnPath);
    startNodes.removeAll(nodesOnPath);
    startNodes.addAll(pendingStarts);

    return path;
  }

  private List<String> getJoinStepIds(String stepId) {
    return stepMap.get(stepId).getTransition().getPredecessors();
  }

  /**
   * Build a workflow graph for the given workflow definition.
   *
   * @param workflow source workflow definition
   * @param runtimeDag runtime dag to include all steps and their transitions in the graph
   * @return generated workflow graph.
   */
  @VisibleForTesting
  static WorkflowGraph build(Workflow workflow, Map<String, StepTransition> runtimeDag) {
    Map<String, Step> stepMap =
        workflow.getSteps().stream()
            .filter(step -> runtimeDag.containsKey(step.getId()))
            .collect(
                Collectors.toMap(
                    Step::getId,
                    step -> {
                      ((AbstractStep) step).setTransition(runtimeDag.get(step.getId()));
                      return step;
                    },
                    (step1, step2) -> {
                      throw new IllegalArgumentException(
                          String.format(
                              "Invalid runtime dag of workflow [%s], where two steps have the same id [%s]",
                              workflow.getId(), step1.getId()));
                    }));

    Map<String, GraphNode> nodeMap = computeNodeMap(workflow.getId(), stepMap);

    final Set<GraphNode> startNodes = new LinkedHashSet<>();
    final List<String> endNodeIds = new ArrayList<>();
    for (GraphNode node : nodeMap.values()) {
      // add predecessors if empty
      if (stepMap.get(node.stepId).getTransition().getPredecessors().isEmpty()) {
        stepMap.get(node.stepId).getTransition().getPredecessors().addAll(node.parents.keySet());
      }
      if (node.parents.isEmpty()) {
        startNodes.add(node);
      }
      if (node.children.isEmpty()) {
        endNodeIds.add(node.stepId);
      }
    }

    return new WorkflowGraph(startNodes, endNodeIds, stepMap);
  }

  /**
   * Build a DAG including all step ids and their transitional relations for the given workflow
   * definition and optional start step ids and end step ids.
   *
   * @param workflow source workflow definition
   * @param startStepIds root step ids that the graph should begin with (inclusively)
   * @param endStepIds end step ids that the graph should end with (inclusively)
   * @return generated DAG including all step ids and their transitional relations.
   */
  public static Map<String, StepTransition> computeDag(
      Workflow workflow, List<String> startStepIds, List<String> endStepIds) {
    Map<String, Step> stepMap =
        workflow.getSteps().stream()
            .collect(
                Collectors.toMap(
                    Step::getId,
                    Function.identity(),
                    (step1, step2) -> {
                      throw new IllegalArgumentException(
                          String.format(
                              "Invalid definition of workflow [%s], where two steps have the same id [%s]",
                              workflow.getId(), step1.getId()));
                    }));

    if (startStepIds != null) {
      Map<String, Step> visited = new HashMap<>();
      Queue<Step> queue = new ArrayDeque<>();
      for (String stepId : startStepIds) {
        Step step =
            Checks.notNull(
                stepMap.get(stepId),
                "Cannot start the graph from step id [%s] as workflow does not contain it.",
                stepId);
        step.getTransition().getPredecessors().clear();
        visited.put(step.getId(), step);
        queue.add(step);
      }
      if (!ObjectHelper.isCollectionEmptyOrNull(endStepIds)) {
        for (String stepId : endStepIds) {
          Step step =
              Checks.notNull(
                  stepMap.get(stepId),
                  "Cannot end the graph with step id [%s] as workflow does not contain it.",
                  stepId);
          step.getTransition().getSuccessors().clear();
          visited.put(step.getId(), step);
        }
      }

      while (!queue.isEmpty()) {
        Step step = queue.remove();
        for (String successor : step.getTransition().getSuccessors().keySet()) {
          if (!visited.containsKey(successor)) {
            Step toAdd = stepMap.get(successor);
            queue.add(toAdd);
            visited.put(toAdd.getId(), toAdd);
          }
        }
      }
      stepMap = visited;
    }

    Map<String, GraphNode> nodeMap = computeNodeMap(workflow.getId(), stepMap);

    for (GraphNode node : nodeMap.values()) {
      // add predecessors if empty
      if (stepMap.get(node.stepId).getTransition().getPredecessors().isEmpty()) {
        stepMap.get(node.stepId).getTransition().getPredecessors().addAll(node.parents.keySet());
      }
    }

    Checks.checkTrue(
        !containsCycleInDag(nodeMap),
        "Invalid workflow definition [%s], where DAG contains cycle",
        workflow.getId());

    return stepMap.values().stream().collect(MapHelper.toListMap(Step::getId, Step::getTransition));
  }

  /** Detect cycle in a DAG using topological sorting algorithm. */
  private static boolean containsCycleInDag(Map<String, GraphNode> nodeMap) {
    Queue<String> queue = new ArrayDeque<>();
    nodeMap.forEach(
        (stepId, node) -> {
          if (node.parents.isEmpty()) {
            queue.add(stepId);
          }
        });
    int visited = 0;
    while (!queue.isEmpty()) {
      String stepId = queue.remove();
      visited++;
      GraphNode node = nodeMap.get(stepId);
      for (String childId : node.children.keySet()) {
        nodeMap.get(childId).parents.remove(stepId);
        if (nodeMap.get(childId).parents.isEmpty()) {
          queue.add(childId);
        }
      }
    }
    return visited != nodeMap.size();
  }

  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
  private static Map<String, GraphNode> computeNodeMap(
      String workflowId, Map<String, Step> stepMap) {
    Map<String, GraphNode> nodeMap = new HashMap<>();
    for (Step step : stepMap.values()) {
      String parent = step.getId();
      if (!nodeMap.containsKey(parent)) {
        nodeMap.put(parent, new GraphNode(parent));
      }
      for (String child : step.getTransition().getSuccessors().keySet()) {
        if (!nodeMap.containsKey(child)) {
          nodeMap.put(child, new GraphNode(child));
        }
        nodeMap.get(parent).addChild(nodeMap.get(child));
        nodeMap.get(child).addParent(nodeMap.get(parent));
      }
    }
    Checks.checkTrue(
        nodeMap.size() == stepMap.size(),
        "Invalid workflow definition [%s], step number mismatch between steps [%s] and dag definition [%s]",
        workflowId,
        stepMap.size(),
        nodeMap.size());
    return nodeMap;
  }
}
