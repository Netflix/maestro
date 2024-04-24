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
package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.junit.BeforeClass;
import org.junit.Test;

public class ForeachDetailsTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Data
  private static class TestDetails {
    ForeachDetails test1;
    ForeachDetails test2;
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);
    assertEquals(
        testDetails, MAPPER.readValue(MAPPER.writeValueAsString(testDetails), TestDetails.class));
  }

  @Test
  public void testAddRefresh() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);
    testDetails.test2.add(21, WorkflowInstance.Status.SUCCEEDED);
    testDetails.test2.add(2, WorkflowInstance.Status.IN_PROGRESS);
    testDetails.test2.add(3, WorkflowInstance.Status.IN_PROGRESS);
    testDetails.test2.add(4, WorkflowInstance.Status.IN_PROGRESS);
    testDetails.test2.add(5, WorkflowInstance.Status.IN_PROGRESS);
    testDetails.test2.refresh();
    assertEquals(
        Collections.singletonList(new ForeachDetails.Interval(2L, 5L)),
        testDetails.test2.getInfo().get(WorkflowInstance.Status.IN_PROGRESS));
    assertEquals(
        Arrays.asList(2L, 5L),
        testDetails.test2.getInfo().get(WorkflowInstance.Status.IN_PROGRESS).get(0).toJson());
    assertEquals(
        Arrays.asList(new ForeachDetails.Interval(10L, 10L), new ForeachDetails.Interval(16L, 22L)),
        testDetails.test2.getInfo().get(WorkflowInstance.Status.SUCCEEDED));
  }

  @Test
  public void testFlatten() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);
    Map<WorkflowInstance.Status, List<Long>> flatten = testDetails.test1.flatten(e -> true);
    assertEquals(Collections.singletonList(1L), flatten.get(WorkflowInstance.Status.CREATED));
    assertEquals(Arrays.asList(2L, 4L, 21L), flatten.get(WorkflowInstance.Status.IN_PROGRESS));
    assertEquals(Collections.singletonList(5L), flatten.get(WorkflowInstance.Status.PAUSED));
    assertEquals(Collections.singletonList(7L), flatten.get(WorkflowInstance.Status.TIMED_OUT));
    assertEquals(Collections.singletonList(6L), flatten.get(WorkflowInstance.Status.STOPPED));
    assertEquals(
        Arrays.asList(8L, 9L, 11L, 12L, 13L, 14L, 15L),
        flatten.get(WorkflowInstance.Status.FAILED));
    assertEquals(
        Arrays.asList(10L, 16L, 17L, 18L, 19L, 20L, 22L),
        flatten.get(WorkflowInstance.Status.SUCCEEDED));
  }

  @Test
  public void testIsForeachIterationRestartable() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);

    assertFalse(testDetails.test1.isForeachIterationRestartable(1));
    assertFalse(testDetails.test1.isForeachIterationRestartable(2));
    assertFalse(testDetails.test1.isForeachIterationRestartable(5));
    assertTrue(testDetails.test1.isForeachIterationRestartable(6));
    assertTrue(testDetails.test1.isForeachIterationRestartable(7));
    assertTrue(testDetails.test1.isForeachIterationRestartable(10));
    assertTrue(testDetails.test1.isForeachIterationRestartable(17));
    assertTrue(testDetails.test1.isForeachIterationRestartable(12));
  }

  @Test
  public void testResetIterationDetail() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);
    testDetails.test1.resetIterationDetail(
        8, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED);
    assertEquals(
        Collections.singletonList(new ForeachDetails.Interval(8L, 8L)),
        testDetails.test1.getPendingInfo().get(WorkflowInstance.Status.CREATED));
    testDetails.test1.resetIterationDetail(
        9, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED);
    assertEquals(
        Collections.singletonList(new ForeachDetails.Interval(11L, 15L)),
        testDetails.test1.getInfo().get(WorkflowInstance.Status.FAILED));
  }

  @Test
  public void testInvalidResetIterationDetail() throws Exception {
    TestDetails testDetails =
        loadObject("fixtures/instances/sample-foreach-details.json", TestDetails.class);
    AssertHelper.assertThrows(
        "should throw exception for Invalid case",
        IllegalArgumentException.class,
        "Invalid: the restarted iteration [8]'s status [FAILED] is missing",
        () ->
            testDetails.test2.resetIterationDetail(
                8, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED));

    AssertHelper.assertThrows(
        "should throw exception for Invalid case",
        IllegalArgumentException.class,
        "Invalid: the restarted iteration [1] is missing",
        () ->
            testDetails.test1.resetIterationDetail(
                1, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED));
  }
}
