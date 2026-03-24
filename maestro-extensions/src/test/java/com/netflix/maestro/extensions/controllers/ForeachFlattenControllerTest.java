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
package com.netflix.maestro.extensions.controllers;

import static org.junit.Assert.assertThrows;

import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.dao.MaestroForeachFlattenedDao;
import com.netflix.maestro.extensions.models.StepIteration;
import com.netflix.maestro.extensions.models.StepIterationsSummary;
import com.netflix.maestro.models.api.PaginationResult;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ForeachFlattenControllerTest extends ExtensionsBaseTest {
  @Mock private MaestroForeachFlattenedDao dao;

  private ForeachFlattenController controller;

  private StepIteration stepIteration;
  private StepIterationsSummary stepIterationsSummary;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    stepIteration =
        StepIteration.create(
            "wf1", 1, 1, "s", "11-11", "1234", Collections.EMPTY_MAP, new StepRuntimeState());
    stepIterationsSummary = new StepIterationsSummary();
    stepIterationsSummary.setCountByStatus(
        Collections.singletonMap(StepInstance.Status.RUNNING, 10l));
    stepIterationsSummary.setRepresentativeIteration(stepIteration);
    stepIterationsSummary.setLoopParamValues(
        Collections.singletonMap("l", Arrays.asList("1", "2")));
    controller = new ForeachFlattenController(dao, MAPPER);
  }

  @Test
  public void testScanForwardNoCursorAndHasMoreItems() {
    Mockito.when(
            dao.scanStepIterations("wf1", 1, 1, "s", null, 2, true, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(2, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations("wf1", 1, 1, "s", 1, null, null, Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(true, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(false, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testScanBackwardsNoCursorAndHasMoreItems() {
    Mockito.when(
            dao.scanStepIterations("wf1", 1, 1, "s", null, 2, false, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(2, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations("wf1", 1, 1, "s", null, 1, null, Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(false, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(true, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testScanForwardWithCursorAndHasMoreItems() {
    Mockito.when(
            dao.scanStepIterations(
                "wf1", 1, 1, "s", "11-11", 2, true, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(2, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations(
            "wf1", 1, 1, "s", 1, null, "MTEtMTE=", Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(true, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(true, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testScanForwardWithCursorAndHasNoMoreItems() {
    Mockito.when(
            dao.scanStepIterations(
                "wf1", 1, 1, "s", "11-11", 2, true, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(1, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations(
            "wf1", 1, 1, "s", 1, null, "MTEtMTE=", Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(false, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(true, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testScanBackwardsWithCursorAndHasMoreItems() {
    Mockito.when(
            dao.scanStepIterations(
                "wf1", 1, 1, "s", "11-11", 2, false, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(2, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations(
            "wf1", 1, 1, "s", null, 1, "MTEtMTE=", Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(true, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(true, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testScanBackwardsWithCursorAndHasNoMoreItems() {
    Mockito.when(
            dao.scanStepIterations(
                "wf1", 1, 1, "s", "11-11", 2, false, null, Collections.EMPTY_LIST))
        .thenReturn(Collections.nCopies(1, stepIteration));
    PaginationResult<StepIteration> result =
        controller.getStepIterations(
            "wf1", 1, 1, "s", null, 1, "MTEtMTE=", Collections.EMPTY_LIST, null);
    Assert.assertEquals(1, result.getElements().size());
    Assert.assertEquals(true, result.getPageInfo().isHasNextPage());
    Assert.assertEquals(false, result.getPageInfo().isHasPreviousPage());
  }

  @Test
  public void testGetStatsSummary() {
    Mockito.when(dao.getStepIterationsSummary("wf1", 1, 1, "s", true, true, 5))
        .thenReturn(stepIterationsSummary);
    StepIterationsSummary s = controller.getStepIterationsSummary("wf1", 1, 1, "s", true, true, 5);
    Assert.assertEquals(stepIterationsSummary.getCountByStatus(), s.getCountByStatus());
    Assert.assertEquals(stepIteration, s.getRepresentativeIteration());
    Assert.assertEquals(stepIterationsSummary.getLoopParamValues(), s.getLoopParamValues());
  }

  @Test
  public void testStepIterationNotFound() {
    Mockito.when(dao.getStepIteration("wf1", 1, 1, "s", "12-11")).thenReturn(null);
    assertThrows(
        "Step iteration is not found for",
        MaestroNotFoundException.class,
        () -> controller.getStepIteration("wf1", 1L, 1L, "s", "2-1"));
  }

  @Test
  public void testGetStepIteration() {
    Mockito.when(dao.getStepIteration("wf1", 1, 1, "s", "11-11")).thenReturn(stepIteration);
    StepIteration iteration = controller.getStepIteration("wf1", 1L, 1L, "s", "1-1");
    Assert.assertNotNull(iteration);
    Assert.assertEquals(stepIteration.getIterationRank(), iteration.getIterationRank());
    Assert.assertEquals(stepIteration.getStepAttemptSeq(), iteration.getStepAttemptSeq());
  }
}
