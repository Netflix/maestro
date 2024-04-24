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
package com.netflix.maestro.models.timeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class TimelineTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    Timeline expected = loadObject("fixtures/timeline/sample-timeline.json", Timeline.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    Timeline actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), Timeline.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testTimelineLimit() {
    Timeline timeline = new Timeline(null);
    List<TimelineEvent> events =
        IntStream.range(0, 150)
            .mapToObj(i -> TimelineLogEvent.info("message %s event", i))
            .collect(Collectors.toList());
    for (TimelineEvent event : events) {
      timeline.add(event);
    }
    assertEquals(Timeline.TIMELINE_SIZE_LIMIT, timeline.getTimelineEvents().size());
    assertEquals(
        "message " + (events.size() - Timeline.TIMELINE_SIZE_LIMIT) + " event",
        timeline.getTimelineEvents().get(0).getMessage());
    assertEquals(
        "message 149 event",
        timeline
            .getTimelineEvents()
            .get(timeline.getTimelineEvents().size() - 1)
            .asLog()
            .getMessage());

    timeline.getTimelineEvents().clear();
    timeline.addAll(events);
    assertEquals(Timeline.TIMELINE_SIZE_LIMIT, timeline.getTimelineEvents().size());
    assertEquals(
        "message " + (events.size() - Timeline.TIMELINE_SIZE_LIMIT) + " event",
        timeline.getTimelineEvents().get(0).getMessage());
    assertEquals(
        "message 149 event",
        timeline
            .getTimelineEvents()
            .get(timeline.getTimelineEvents().size() - 1)
            .asLog()
            .getMessage());

    Timeline actual = new Timeline(events);
    assertEquals(timeline, actual);
  }

  @Test
  public void testLatestMatched() {
    Timeline timeline = new Timeline(null);
    assertFalse(timeline.latestMatched(TimelineLogEvent.info("test")));
    timeline.add(TimelineLogEvent.info("test"));
    assertTrue(timeline.latestMatched(TimelineLogEvent.info("test")));
    timeline.add(TimelineLogEvent.debug("test"));
    assertFalse(timeline.latestMatched(TimelineLogEvent.info("test")));
    assertEquals(2, timeline.getTimelineEvents().size());
    assertFalse(timeline.latestMatched(TimelineDetailsEvent.from(Details.create("test details"))));
    timeline.add(TimelineDetailsEvent.from(Details.create("test details")));
    assertEquals(3, timeline.getTimelineEvents().size());
    assertTrue(timeline.latestMatched(TimelineDetailsEvent.from(Details.create("test details"))));
  }

  @Test
  public void testConstructor() {
    Timeline timeline =
        new Timeline(
            Arrays.asList(
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test")));
    assertEquals(1, timeline.getTimelineEvents().size());
    assertTrue(timeline.getTimelineEvents().get(0).isIdentical(TimelineLogEvent.info("test")));
  }

  @Test
  public void testAddAndAddAll() {
    Timeline timeline = new Timeline(null);
    assertTrue(timeline.addAll(Collections.singletonList(TimelineLogEvent.info("test"))));
    assertEquals(1, timeline.getTimelineEvents().size());
    assertFalse(
        timeline.addAll(
            Arrays.asList(
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"))));
    assertEquals(1, timeline.getTimelineEvents().size());
    assertTrue(timeline.add(TimelineLogEvent.debug("test")));
    assertEquals(2, timeline.getTimelineEvents().size());
    assertTrue(
        timeline.addAll(
            Arrays.asList(
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"))));
    assertEquals(3, timeline.getTimelineEvents().size());
    assertFalse(timeline.latestMatched(TimelineDetailsEvent.from(Details.create("test details"))));
    timeline.add(TimelineDetailsEvent.from(Details.create("test details")));
    assertEquals(4, timeline.getTimelineEvents().size());
    assertFalse(timeline.add(TimelineDetailsEvent.from(Details.create("test details"))));
    assertEquals(4, timeline.getTimelineEvents().size());
    assertTrue(
        timeline.addAll(
            Arrays.asList(
                TimelineDetailsEvent.from(Details.create("test details")),
                TimelineLogEvent.info("test"),
                TimelineLogEvent.info("test"))));
    assertEquals(5, timeline.getTimelineEvents().size());
    List<TimelineEvent> expected =
        Arrays.asList(
            TimelineLogEvent.info("test"),
            TimelineLogEvent.debug("test"),
            TimelineLogEvent.info("test"),
            TimelineDetailsEvent.from(Details.create("test details")),
            TimelineLogEvent.info("test"));
    IntStream.range(0, 5)
        .forEach(i -> assertTrue(timeline.getTimelineEvents().get(i).isIdentical(expected.get(i))));
  }

  @Test
  public void testGetEnrichedWorkflowInstance() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setCreateTime(1608749932076L);
    instance.setStartTime(1608749932077L);
    instance.setEndTime(1608749932078L);
    instance.setTimeline(new Timeline(null));
    instance.getTimeline().add(TimelineLogEvent.info("hello world"));
    instance.getTimeline().add(TimelineDetailsEvent.from(Details.create("sample error details")));
    instance.setStatus(WorkflowInstance.Status.FAILED);
    instance.enrich();
    assertEquals(
        TimelineStatusEvent.create(1608749932076L, "CREATED"),
        instance.getTimeline().getTimelineEvents().get(0));
    assertEquals(
        TimelineStatusEvent.create(1608749932077L, "IN_PROGRESS"),
        instance.getTimeline().getTimelineEvents().get(1));
    assertEquals(
        TimelineStatusEvent.create(1608749932078L, "FAILED"),
        instance.getTimeline().getTimelineEvents().get(2));
    assertEquals("hello world", instance.getTimeline().getTimelineEvents().get(3).getMessage());
    assertEquals(
        "sample error details", instance.getTimeline().getTimelineEvents().get(4).getMessage());
  }

  @Test
  public void testGetEnrichedStepInstance() throws Exception {
    StepInstance instance =
        loadObject("fixtures/instances/sample-step-instance-succeeded.json", StepInstance.class);
    Timeline timeline = instance.getTimeline();
    timeline.enrich(instance);

    assertEquals(12, instance.getTimeline().getTimelineEvents().size());
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
        TimelineStatusEvent.create(1608749934142L, "EVALUATING_PARAMS"),
        instance.getTimeline().getTimelineEvents().get(4));
    assertEquals(
        TimelineStatusEvent.create(1608749934142L, "WAITING_FOR_PERMITS"),
        instance.getTimeline().getTimelineEvents().get(5));
    assertEquals(
        TimelineStatusEvent.create(1608749934142L, "STARTING"),
        instance.getTimeline().getTimelineEvents().get(6));
    assertEquals(
        TimelineStatusEvent.create(1608749934147L, "RUNNING"),
        instance.getTimeline().getTimelineEvents().get(7));
    assertEquals(
        TimelineStatusEvent.create(1608749950263L, "FINISHING"),
        instance.getTimeline().getTimelineEvents().get(8));
    assertEquals(
        TimelineStatusEvent.create(1608749950263L, "SUCCEEDED"),
        instance.getTimeline().getTimelineEvents().get(9));
    assertEquals("hello world", instance.getTimeline().getTimelineEvents().get(10).getMessage());
    assertEquals(
        "sample error details", instance.getTimeline().getTimelineEvents().get(11).getMessage());
  }
}
