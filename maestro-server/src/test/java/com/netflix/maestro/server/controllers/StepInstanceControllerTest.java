package com.netflix.maestro.server.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.models.timeline.TimelineStatusEvent;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class StepInstanceControllerTest extends MaestroBaseTest {
  @Mock private MaestroStepInstanceDao mockInstanceDao;
  private StepInstanceController stepInstanceController;

  @Before
  public void before() {
    this.mockInstanceDao = mock(MaestroStepInstanceDao.class);
    this.stepInstanceController = new StepInstanceController(this.mockInstanceDao);
  }

  @Test
  public void testGetStepInstanceView() {
    StepInstance instance = mock(StepInstance.class);
    when(mockInstanceDao.getStepInstanceView("test-workflow", 1, "job1")).thenReturn(instance);
    stepInstanceController.getStepInstanceView("test-workflow", 1, "job1", true);
    verify(mockInstanceDao, times(1)).getStepInstanceView("test-workflow", 1, "job1");
    verify(instance, times(1)).enrich();
  }

  @Test
  public void testGetStepInstance() {
    StepInstance instance = mock(StepInstance.class);
    when(mockInstanceDao.getStepInstance("test-workflow", 1, 1, "job1", "1")).thenReturn(instance);
    stepInstanceController.getStepInstance("test-workflow", 1, 1, "job1", "1", true);
    verify(mockInstanceDao, times(1)).getStepInstance("test-workflow", 1, 1, "job1", "1");
    verify(instance, times(1)).enrich();
  }

  @Test
  public void testGetEnrichedStepInstance() {
    StepInstance instance = new StepInstance();
    instance.setTimeline(new Timeline(null));
    instance.getTimeline().add(TimelineLogEvent.info("hello world"));
    instance.getTimeline().add(TimelineDetailsEvent.from(Details.create("sample error details")));
    StepRuntimeState state = new StepRuntimeState();
    state.setCreateTime(1608749932076L);
    state.setInitializeTime(1608749932078L);
    state.setPauseTime(1608749932079L);
    state.setWaitSignalTime(1608749934142L);
    state.setWaitPermitTime(1608749934142L);
    state.setStartTime(1608749934142L);
    state.setExecuteTime(1608749934147L);
    state.setFinishTime(1608749950263L);
    state.setEndTime(1608749950263L);
    state.setStatus(StepInstance.Status.SUCCEEDED);
    instance.setRuntimeState(state);
    when(mockInstanceDao.getStepInstance("test-workflow", 1, 1, "job1", "1")).thenReturn(instance);
    stepInstanceController.getStepInstance("test-workflow", 1, 1, "job1", "1", true);
    assertEquals(11, instance.getTimeline().getTimelineEvents().size());
    assertEquals(
        TimelineStatusEvent.create(1608749932076L, "CREATED"),
        instance.getTimeline().getTimelineEvents().get(0));
    assertEquals(
        TimelineStatusEvent.create(1608749932078L, "INITIALIZED"),
        instance.getTimeline().getTimelineEvents().get(1));
    assertEquals(
        TimelineStatusEvent.create(1608749932079L, "PAUSED"),
        instance.getTimeline().getTimelineEvents().get(2));
    assertEquals(
        TimelineStatusEvent.create(1608749934142L, "WAITING_FOR_SIGNALS"),
        instance.getTimeline().getTimelineEvents().get(3));
    assertEquals(
        TimelineStatusEvent.create(1608749934142L, "WAITING_FOR_PERMITS"),
        instance.getTimeline().getTimelineEvents().get(4));
    assertEquals(
        TimelineStatusEvent.create(1608749934142L, "STARTING"),
        instance.getTimeline().getTimelineEvents().get(5));
    assertEquals(
        TimelineStatusEvent.create(1608749934147L, "RUNNING"),
        instance.getTimeline().getTimelineEvents().get(6));
    assertEquals(
        TimelineStatusEvent.create(1608749950263L, "FINISHING"),
        instance.getTimeline().getTimelineEvents().get(7));
    assertEquals(
        TimelineStatusEvent.create(1608749950263L, "SUCCEEDED"),
        instance.getTimeline().getTimelineEvents().get(8));
    assertEquals("hello world", instance.getTimeline().getTimelineEvents().get(9).getMessage());
    assertEquals(
        "sample error details", instance.getTimeline().getTimelineEvents().get(10).getMessage());
  }

  @Test
  public void testGetStepInstanceLatestAttempt() {
    StepInstance instance = mock(StepInstance.class);
    when(mockInstanceDao.getStepInstance("test-workflow", 1, 1, "job1", "latest"))
        .thenReturn(instance);
    stepInstanceController.getStepInstance("test-workflow", 1, 1, "job1", "latest", false);
    verify(mockInstanceDao, times(1)).getStepInstance("test-workflow", 1, 1, "job1", "latest");
    verify(instance, times(0)).enrich();
  }

  @Test
  public void testGetStepInstanceViews() {
    StepInstance instance1 = mock(StepInstance.class);
    StepInstance instance2 = mock(StepInstance.class);
    when(instance1.getStepInstanceId()).thenReturn(1L);
    when(instance2.getStepInstanceId()).thenReturn(1L);
    when(instance1.getStepAttemptId()).thenReturn(1L);
    when(instance2.getStepAttemptId()).thenReturn(2L);
    when(mockInstanceDao.getAllStepInstances("test-workflow", 1, 1))
        .thenReturn(Arrays.asList(instance1, instance2));
    List<StepInstance> ret = stepInstanceController.getStepInstanceViews("test-workflow", 1, 1);
    verify(mockInstanceDao, times(1)).getAllStepInstances("test-workflow", 1, 1);
    assertEquals(1, ret.size());
    assertEquals(instance2, ret.get(0));
  }
}
