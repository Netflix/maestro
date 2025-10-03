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
package com.netflix.maestro.engine.tasks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroTagPermitDao;
import com.netflix.maestro.engine.dto.StepTagPermit;
import com.netflix.maestro.engine.dto.StepUuidSeq;
import com.netflix.maestro.engine.properties.TagPermitTaskProperties;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.tagpermits.TagPermit;
import com.netflix.maestro.models.timeline.TimelineActionEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroTagPermitTaskTest extends MaestroEngineBaseTest {
  @Mock private MaestroTagPermitDao tagPermitDao;
  @Mock private TagPermitTaskProperties properties;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private Flow flow;
  @Mock private Task task;

  private MaestroTagPermitTask tagPermitTask;

  @Before
  public void setUp() {
    when(properties.getBatchSize()).thenReturn(2);
    when(properties.getScanInterval()).thenReturn(5000L);
    when(properties.getCleanUpInterval()).thenReturn(30000L);

    tagPermitTask = new MaestroTagPermitTask(tagPermitDao, properties, queueSystem, metricRepo);

    Map<String, Object> outputData = new HashMap<>();
    when(task.getOutputData()).thenReturn(outputData);
    when(task.getCode()).thenReturn(0);
  }

  @Test
  public void testStartLoadTagPermitsAndStepTagPermits() {
    List<TagPermit> tagPermits =
        List.of(new TagPermit("tag1", 5, null), new TagPermit("tag2", 3, null));
    when(tagPermitDao.getSyncedTagPermits(anyString(), anyInt()))
        .thenReturn(tagPermits)
        .thenReturn(List.of());

    List<StepTagPermit> stepTagPermits =
        List.of(
            new StepTagPermit(
                UUID.randomUUID(),
                1L,
                MaestroTagPermitTask.ACQUIRED_STATUS_CODE,
                new String[] {"tag1"},
                null,
                null),
            new StepTagPermit(
                UUID.randomUUID(), 2L, 6, new String[] {"tag2"}, new Integer[] {3}, null));
    when(tagPermitDao.loadStepTagPermits(any(), anyInt()))
        .thenReturn(stepTagPermits)
        .thenReturn(List.of());

    tagPermitTask.start(flow, task);

    verify(tagPermitDao, times(2)).getSyncedTagPermits(anyString(), anyInt());
    verify(tagPermitDao, times(1)).getSyncedTagPermits("", 2);
    verify(tagPermitDao, times(1)).getSyncedTagPermits("tag2", 2);
    verify(tagPermitDao, times(2)).loadStepTagPermits(any(), anyInt());
    assertNotNull(task.getOutputData().get(Constants.STEP_RUNTIME_SUMMARY_FIELD));
  }

  private void testActionCode(int code, int num1, int num2, int num3, int num4) {
    tagPermitTask.start(flow, task);
    when(task.getCode()).thenReturn(code);

    when(tagPermitDao.markAndLoadTagPermits(anyInt())).thenReturn(Collections.emptyList());
    when(tagPermitDao.markAndLoadStepTagPermits(anyLong(), anyInt()))
        .thenReturn(Collections.emptyList());
    when(tagPermitDao.removeTagPermits(anyInt()))
        .thenReturn(Arrays.asList("tag1", "tag2"))
        .thenReturn(Collections.emptyList());
    when(tagPermitDao.removeReleasedStepTagPermits(anyInt()))
        .thenReturn(
            Arrays.asList(
                new StepUuidSeq(UUID.randomUUID(), 1), new StepUuidSeq(UUID.randomUUID(), 2)))
        .thenReturn(Collections.emptyList());
    when(properties.getCleanUpInterval()).thenReturn(Long.MAX_VALUE);

    boolean result = tagPermitTask.execute(flow, task);

    assertFalse(result);
    verify(tagPermitDao, times(num1)).markAndLoadTagPermits(anyInt());
    verify(tagPermitDao, times(num2)).markAndLoadStepTagPermits(anyLong(), anyInt());
    verify(tagPermitDao, times(num3)).removeTagPermits(anyInt());
    verify(tagPermitDao, times(num4)).removeReleasedStepTagPermits(anyInt());
    verify(task).setStartDelayInMillis(5000L);
  }

  @Test
  public void testExecuteDefaultActionCode() {
    testActionCode(MaestroTagPermitTask.DEFAULT_ACTION_CODE, 1, 1, 0, 0);
  }

  @Test
  public void testExecuteTagPermitChangeCode() {
    testActionCode(MaestroTagPermitTask.TAG_PERMIT_CHANGE_CODE, 1, 0, 0, 0);
  }

  @Test
  public void testExecuteStepTagPermitChangeCode() {
    testActionCode(MaestroTagPermitTask.STEP_TAG_PERMIT_CHANGE_CODE, 0, 1, 0, 0);
  }

  @Test
  public void testExecuteTagPermitDeleteCode() {
    testActionCode(MaestroTagPermitTask.TAG_PERMIT_DELETE_CODE, 0, 0, 2, 0);
  }

  @Test
  public void testExecuteStepTagPermitDeleteCode() {
    testActionCode(MaestroTagPermitTask.STEP_TAG_PERMIT_DELETE_CODE, 0, 0, 0, 2);
  }

  @Test
  public void testTagPermitAssignmentWithAvailablePermits() {
    List<TagPermit> tagPermits = List.of(new TagPermit("tag1", 2, null));
    when(tagPermitDao.getSyncedTagPermits(anyString(), anyInt())).thenReturn(tagPermits);

    UUID stepUuid = UUID.randomUUID();
    List<StepTagPermit> stepTagPermits =
        List.of(
            new StepTagPermit(
                stepUuid,
                1L,
                6, // queued status
                new String[] {"tag1"},
                new Integer[] {2},
                TimelineActionEvent.builder()
                    .action("test")
                    .author(User.builder().name("test-user").build())
                    .message("test-message")
                    .info(123L)
                    .build()));
    when(tagPermitDao.loadStepTagPermits(any(), anyInt()))
        .thenReturn(stepTagPermits)
        .thenReturn(Collections.emptyList());

    tagPermitTask.start(flow, task);
    tagPermitTask.execute(flow, task);

    verify(tagPermitDao, times(1)).markStepTagPermitAcquired(stepUuid);
    verify(queueSystem, times(0)).notify(any());

    when(tagPermitDao.markStepTagPermitAcquired(any())).thenReturn(true);
    tagPermitTask.execute(flow, task);
    verify(tagPermitDao, times(2)).markStepTagPermitAcquired(stepUuid);
    verify(queueSystem, times(1)).notify(any());
  }

  @Test
  public void testTagPermitAssignmentWithLimitReached() {
    List<TagPermit> tagPermits = List.of(new TagPermit("tag1", 1, null));
    when(tagPermitDao.getSyncedTagPermits(anyString(), anyInt())).thenReturn(tagPermits);

    UUID acquiredUuid = UUID.randomUUID();
    UUID queuedUuid = UUID.randomUUID();
    List<StepTagPermit> stepTagPermits =
        Arrays.asList(
            new StepTagPermit(
                acquiredUuid,
                1L,
                MaestroTagPermitTask.ACQUIRED_STATUS_CODE,
                new String[] {"tag1"},
                null,
                null),
            new StepTagPermit(
                queuedUuid,
                2L,
                6, // queued status
                new String[] {"tag1"},
                new Integer[] {1},
                null));
    when(tagPermitDao.loadStepTagPermits(any(), anyInt()))
        .thenReturn(stepTagPermits)
        .thenReturn(Collections.emptyList());

    tagPermitTask.start(flow, task);
    tagPermitTask.execute(flow, task);

    verify(tagPermitDao, never()).markStepTagPermitAcquired(queuedUuid);
  }

  @Test
  public void testCleanupIntervalTriggered() {
    tagPermitTask.start(flow, task);
    when(tagPermitDao.removeTagPermits(anyInt())).thenReturn(Collections.emptyList());
    when(tagPermitDao.removeReleasedStepTagPermits(anyInt())).thenReturn(Collections.emptyList());

    tagPermitTask.execute(flow, task);

    verify(tagPermitDao, times(1)).removeTagPermits(anyInt());
    verify(tagPermitDao, times(1)).removeReleasedStepTagPermits(anyInt());
  }
}
