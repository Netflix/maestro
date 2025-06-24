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
package com.netflix.maestro.server.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.api.PaginationDirection;
import com.netflix.maestro.models.api.PaginationResult;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class PaginationHelperTest {

  @Test
  public void testGetPaginationRangeCursorNull() {
    PaginationHelper.PaginationRange paginationRange =
        PaginationHelper.getPaginationRange(null, PaginationDirection.NEXT, 1, 10, 5);
    assertEquals(6, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("", PaginationDirection.NEXT, 1, 10, 5);
    assertEquals(6, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange(null, PaginationDirection.PREV, 1, 10, 5);
    assertEquals(1, paginationRange.start());
    assertEquals(5, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("", PaginationDirection.PREV, 1, 10, 5);
    assertEquals(1, paginationRange.start());
    assertEquals(5, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange(null, PaginationDirection.NEXT, 8, 10, 5);
    assertEquals(8, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("", PaginationDirection.NEXT, 8, 10, 5);
    assertEquals(8, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange(null, PaginationDirection.PREV, 8, 10, 5);
    assertEquals(8, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("", PaginationDirection.PREV, 8, 10, 5);
    assertEquals(8, paginationRange.start());
    assertEquals(10, paginationRange.end());
  }

  @Test
  public void testGetPaginationRangeCursorExists() {
    PaginationHelper.PaginationRange paginationRange =
        PaginationHelper.getPaginationRange("5", PaginationDirection.NEXT, 1, 10, 5);
    assertEquals(1, paginationRange.start());
    assertEquals(4, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("5", PaginationDirection.PREV, 1, 10, 5);
    assertEquals(6, paginationRange.start());
    assertEquals(10, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("5", PaginationDirection.NEXT, 4, 10, 5);
    assertEquals(4, paginationRange.start());
    assertEquals(4, paginationRange.end());

    paginationRange = PaginationHelper.getPaginationRange("8", PaginationDirection.PREV, 7, 10, 5);
    assertEquals(9, paginationRange.start());
    assertEquals(10, paginationRange.end());
  }

  @Test
  public void testBuildPaginationResultWithEmptyItems() {
    PaginationResult<WorkflowInstance> paginationResult =
        PaginationHelper.buildPaginationResult(
            new ArrayList<>(), 10, 1, (items) -> new long[] {10L, 0L});

    assertNull(paginationResult.getPageInfo().getStartCursor());
    assertNull(paginationResult.getPageInfo().getEndCursor());
    assertNull(paginationResult.getPageInfo().getStartIndex());
    assertFalse(paginationResult.getPageInfo().isHasPreviousPage());
    assertFalse(paginationResult.getPageInfo().isHasNextPage());
    assertEquals(10, paginationResult.getTotalCount());
    assertTrue(paginationResult.getElements().isEmpty());
  }

  @Test
  public void testBuildPaginationResultWithNonEmptyLastPage() {
    WorkflowInstance workflowInstance1 = mock(WorkflowInstance.class);
    when(workflowInstance1.getWorkflowInstanceId()).thenReturn(1L);
    WorkflowInstance workflowInstance2 = mock(WorkflowInstance.class);
    when(workflowInstance2.getWorkflowInstanceId()).thenReturn(2L);
    WorkflowInstance workflowInstance3 = mock(WorkflowInstance.class);
    when(workflowInstance3.getWorkflowInstanceId()).thenReturn(3L);

    // sort on the workflow instance id
    List<WorkflowInstance> workflowInstanceList =
        Arrays.asList(workflowInstance3, workflowInstance2, workflowInstance1);
    PaginationResult<WorkflowInstance> paginationResult =
        PaginationHelper.buildPaginationResult(
            workflowInstanceList, 10, 1, (instances) -> new long[] {3L, 1L});

    assertEquals("3", paginationResult.getPageInfo().getStartCursor());
    assertEquals("1", paginationResult.getPageInfo().getEndCursor());
    assertEquals(8L, paginationResult.getPageInfo().getStartIndex().longValue());
    assertTrue(paginationResult.getPageInfo().isHasPreviousPage());
    assertFalse(paginationResult.getPageInfo().isHasNextPage());
    assertEquals(10, paginationResult.getTotalCount());
    assertEquals(3, paginationResult.getElements().size());
    assertEquals(3L, paginationResult.getElements().get(0).getWorkflowInstanceId());
    assertEquals(2L, paginationResult.getElements().get(1).getWorkflowInstanceId());
    assertEquals(1L, paginationResult.getElements().get(2).getWorkflowInstanceId());
  }

  @Test
  public void testBuildPaginationResultWithNonEmptyFirstPage() {
    WorkflowInstance workflowInstance1 = mock(WorkflowInstance.class);
    when(workflowInstance1.getWorkflowInstanceId()).thenReturn(10L);
    WorkflowInstance workflowInstance2 = mock(WorkflowInstance.class);
    when(workflowInstance2.getWorkflowInstanceId()).thenReturn(9L);
    WorkflowInstance workflowInstance3 = mock(WorkflowInstance.class);
    when(workflowInstance3.getWorkflowInstanceId()).thenReturn(8L);

    // sort on the workflow instance id
    List<WorkflowInstance> workflowInstanceList =
        Arrays.asList(workflowInstance1, workflowInstance2, workflowInstance3);

    PaginationResult<WorkflowInstance> paginationResult =
        PaginationHelper.buildPaginationResult(
            workflowInstanceList, 10, 1, (instances) -> new long[] {10L, 8L});

    assertEquals("10", paginationResult.getPageInfo().getStartCursor());
    assertEquals("8", paginationResult.getPageInfo().getEndCursor());
    assertEquals(1L, paginationResult.getPageInfo().getStartIndex().longValue());
    assertFalse(paginationResult.getPageInfo().isHasPreviousPage());
    assertTrue(paginationResult.getPageInfo().isHasNextPage());
    assertEquals(10, paginationResult.getTotalCount());
    assertEquals(3, paginationResult.getElements().size());
    assertEquals(10L, paginationResult.getElements().get(0).getWorkflowInstanceId());
    assertEquals(9L, paginationResult.getElements().get(1).getWorkflowInstanceId());
    assertEquals(8L, paginationResult.getElements().get(2).getWorkflowInstanceId());
  }

  @Test
  public void testBuildPaginationResultWithNonEmptyMiddlePage() {
    WorkflowInstance workflowInstance1 = mock(WorkflowInstance.class);
    when(workflowInstance1.getWorkflowInstanceId()).thenReturn(6L);
    WorkflowInstance workflowInstance2 = mock(WorkflowInstance.class);
    when(workflowInstance2.getWorkflowInstanceId()).thenReturn(5L);
    WorkflowInstance workflowInstance3 = mock(WorkflowInstance.class);
    when(workflowInstance3.getWorkflowInstanceId()).thenReturn(4L);

    // unsorted on the workflow instance id
    List<WorkflowInstance> workflowInstanceList =
        Arrays.asList(workflowInstance1, workflowInstance2, workflowInstance3);

    PaginationResult<WorkflowInstance> paginationResult =
        PaginationHelper.buildPaginationResult(
            workflowInstanceList, 10, 1, (instances) -> new long[] {6L, 4L});

    assertEquals("6", paginationResult.getPageInfo().getStartCursor());
    assertEquals("4", paginationResult.getPageInfo().getEndCursor());
    assertEquals(5L, paginationResult.getPageInfo().getStartIndex().longValue());
    assertTrue(paginationResult.getPageInfo().isHasPreviousPage());
    assertTrue(paginationResult.getPageInfo().isHasNextPage());
    assertEquals(10, paginationResult.getTotalCount());
    assertEquals(3, paginationResult.getElements().size());
    assertEquals(6L, paginationResult.getElements().get(0).getWorkflowInstanceId());
    assertEquals(5L, paginationResult.getElements().get(1).getWorkflowInstanceId());
    assertEquals(4L, paginationResult.getElements().get(2).getWorkflowInstanceId());
  }

  @Test
  public void testValidateParamAndDeriveDirection() {
    AssertHelper.assertThrows(
        "should fail as both params are null",
        MaestroValidationException.class,
        "Either first or last need to be provided, both cannot be null",
        () -> PaginationHelper.validateParamAndDeriveDirection(null, null));
    AssertHelper.assertThrows(
        "should fail as both params are provided",
        MaestroValidationException.class,
        "Either first or last need to be provided, but not both",
        () -> PaginationHelper.validateParamAndDeriveDirection(1L, 10L));
    PaginationDirection direction = PaginationHelper.validateParamAndDeriveDirection(1L, null);
    assertEquals(PaginationDirection.NEXT, direction);
    direction = PaginationHelper.validateParamAndDeriveDirection(null, 10L);
    assertEquals(PaginationDirection.PREV, direction);
  }

  @Test
  public void testBuildPaginationResultWithHigherEarlier() {
    WorkflowInstance workflowInstance1 = mock(WorkflowInstance.class);
    when(workflowInstance1.getWorkflowInstanceId()).thenReturn(6L);
    WorkflowInstance workflowInstance2 = mock(WorkflowInstance.class);
    when(workflowInstance2.getWorkflowInstanceId()).thenReturn(5L);
    WorkflowInstance workflowInstance3 = mock(WorkflowInstance.class);
    when(workflowInstance3.getWorkflowInstanceId()).thenReturn(4L);

    // unsorted on the workflow instance id
    List<WorkflowInstance> workflowInstanceList =
        Arrays.asList(workflowInstance1, workflowInstance2, workflowInstance3);

    PaginationResult<WorkflowInstance> paginationResult =
        PaginationHelper.buildPaginationResult(
            workflowInstanceList, 10, 4, (instances) -> new long[] {6L, 4L});

    assertEquals("6", paginationResult.getPageInfo().getStartCursor());
    assertEquals("4", paginationResult.getPageInfo().getEndCursor());
    assertEquals(5L, paginationResult.getPageInfo().getStartIndex().longValue());
    assertTrue(paginationResult.getPageInfo().isHasPreviousPage());
    assertFalse(paginationResult.getPageInfo().isHasNextPage());
    assertEquals(7, paginationResult.getTotalCount());
    assertEquals(3, paginationResult.getElements().size());
    assertEquals(6L, paginationResult.getElements().get(0).getWorkflowInstanceId());
    assertEquals(5L, paginationResult.getElements().get(1).getWorkflowInstanceId());
    assertEquals(4L, paginationResult.getElements().get(2).getWorkflowInstanceId());
  }

  @Test
  public void testGetPaginationRangeEdgeCases() {
    // Test single item range
    PaginationHelper.PaginationRange range =
        PaginationHelper.getPaginationRange(null, PaginationDirection.NEXT, 5, 5, 10);
    assertEquals(5, range.start());
    assertEquals(5, range.end());

    // Test limit larger than range
    range = PaginationHelper.getPaginationRange(null, PaginationDirection.NEXT, 1, 3, 10);
    assertEquals(1, range.start());
    assertEquals(3, range.end());

    // Test cursor at boundary
    range = PaginationHelper.getPaginationRange("1", PaginationDirection.NEXT, 1, 10, 5);
    assertEquals(0, range.start());
    assertEquals(0, range.end());
  }

  @Test
  public void testBuildPaginationResultWithSingleItem() {
    WorkflowInstance workflowInstance = mock(WorkflowInstance.class);
    when(workflowInstance.getWorkflowInstanceId()).thenReturn(5L);
    List<WorkflowInstance> instances = List.of(workflowInstance);

    PaginationResult<WorkflowInstance> result =
        PaginationHelper.buildPaginationResult(instances, 10, 1, (items) -> new long[] {5L, 5L});

    assertEquals("5", result.getPageInfo().getStartCursor());
    assertEquals("5", result.getPageInfo().getEndCursor());
    assertEquals(6L, result.getPageInfo().getStartIndex().longValue());
    assertTrue(result.getPageInfo().isHasPreviousPage());
    assertTrue(result.getPageInfo().isHasNextPage());
    assertEquals(10, result.getTotalCount());
    assertEquals(1, result.getElements().size());
  }

  @Test
  public void testBuildPaginationResultWithZeroTotalCount() {
    PaginationResult<WorkflowInstance> result =
        PaginationHelper.buildPaginationResult(List.of(), 0, 1, (ignored) -> new long[] {0L, 0L});

    assertNull(result.getPageInfo().getStartCursor());
    assertNull(result.getPageInfo().getEndCursor());
    assertNull(result.getPageInfo().getStartIndex());
    assertFalse(result.getPageInfo().isHasPreviousPage());
    assertFalse(result.getPageInfo().isHasNextPage());
    assertEquals(0, result.getTotalCount());
    assertTrue(result.getElements().isEmpty());
  }
}
