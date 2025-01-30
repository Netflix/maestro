/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.flow.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.flow.FlowBaseTest;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class FlowTest extends FlowBaseTest {

  private Flow flow;
  private Task task;

  @Before
  public void init() {
    flow = createFlow();
    task = flow.newTask(new TaskDef("task1", "noop", null, null), false);
  }

  @Test
  public void testAddFinishedTask() {
    assertEquals(1, flow.getRunningTasks().size());
    AssertHelper.assertThrows(
        "should throw if adding unfinished task",
        IllegalArgumentException.class,
        "must be in the terminal state",
        () -> flow.addFinishedTask(task));
    task.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task);
    flow.addFinishedTask(task);
    flow.addFinishedTask(task);
    assertEquals(1, flow.getFinishedTasks().size());
    assertEquals(0, flow.getRunningTasks().size());
    assertEquals(task, flow.getFinishedTasks().stream().findFirst().get());
  }

  @Test
  public void testUpdateRunningTask() {
    var updatedTask = flow.newTask(new TaskDef("task1", "noop1", null, null), true);
    assertEquals(Set.of("task1"), flow.getRunningTasks().keySet());
    flow.updateRunningTask(updatedTask);
    assertEquals(Map.of("task1", updatedTask), flow.getRunningTasks());
  }

  @Test
  public void testGetFinishedTasks() {
    assertEquals(0, flow.getFinishedTasks().size());
    var snapshot = flow.getFinishedTasks();
    assertTrue(snapshot.isEmpty());

    task.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task);
    assertTrue(snapshot.isEmpty());
    assertEquals(1, flow.getFinishedTasks().size());
  }

  @Test
  public void testGetStreamOfAllTasks() {
    assertEquals(1, flow.getStreamOfAllTasks().count());
    task.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task);
    assertEquals(1, flow.getStreamOfAllTasks().count());
    flow.updateRunningTask(task);
    assertEquals(2, flow.getStreamOfAllTasks().count());
  }

  @Test
  public void testMarkTimedout() {
    flow.markTimedout();
    assertEquals(Flow.Status.TIMED_OUT, flow.getStatus());
  }

  @Test
  public void testNewTask() {
    assertEquals(1, flow.getRunningTasks().size());
    assertEquals(Set.of("task1"), flow.getRunningTasks().keySet());

    var taskDef = new TaskDef("task2", "noop", null, null);
    var task2 = flow.newTask(taskDef, false);
    assertEquals(taskDef, task2.getTaskDef());
    assertTrue(task2.isActive());
    assertFalse(task2.isTerminal());
    assertEquals(2, flow.getRunningTasks().size());
    assertEquals(Set.of("task1", "task2"), flow.getRunningTasks().keySet());
  }

  @Test
  public void testNewTaskInline() {
    assertEquals(1, flow.getRunningTasks().size());
    assertEquals(Set.of("task1"), flow.getRunningTasks().keySet());

    var taskDef = new TaskDef("task2", "noop", null, null);
    var task2 = flow.newTask(taskDef, true);
    assertEquals(taskDef, task2.getTaskDef());
    assertTrue(task2.isActive());
    assertFalse(task2.isTerminal());
    assertEquals(1, flow.getRunningTasks().size());
    assertEquals(Set.of("task1"), flow.getRunningTasks().keySet());
  }
}
