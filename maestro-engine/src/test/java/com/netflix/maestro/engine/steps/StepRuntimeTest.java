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
package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.DefaultArtifact;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class StepRuntimeTest extends MaestroEngineBaseTest {

  @Test
  public void testStateIsFailed() {
    Assert.assertFalse(StepRuntime.State.CONTINUE.isFailed());
    Assert.assertFalse(StepRuntime.State.DONE.isFailed());
    Assert.assertTrue(StepRuntime.State.USER_ERROR.isFailed());
    Assert.assertTrue(StepRuntime.State.PLATFORM_ERROR.isFailed());
    Assert.assertTrue(StepRuntime.State.FATAL_ERROR.isFailed());
    Assert.assertFalse(StepRuntime.State.STOPPED.isFailed());
    Assert.assertTrue(StepRuntime.State.TIMED_OUT.isFailed());
  }

  @Test
  public void testStateIsTerminal() {
    Assert.assertFalse(StepRuntime.State.CONTINUE.isTerminal());
    Assert.assertTrue(StepRuntime.State.DONE.isTerminal());
    Assert.assertTrue(StepRuntime.State.USER_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.PLATFORM_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.FATAL_ERROR.isTerminal());
    Assert.assertTrue(StepRuntime.State.STOPPED.isTerminal());
    Assert.assertTrue(StepRuntime.State.TIMED_OUT.isTerminal());
  }

  @Test
  public void testResultRecord() {
    // Test basic result creation
    StepRuntime.Result result = StepRuntime.Result.of(StepRuntime.State.CONTINUE);
    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertNull(result.nextPollingDelayInMillis());
    assertTrue(result.artifacts().isEmpty());
    assertTrue(result.timeline().isEmpty());

    // Test result with polling interval
    StepRuntime.Result resultWithInterval =
        StepRuntime.Result.of(StepRuntime.State.CONTINUE, 5000L);
    assertEquals(StepRuntime.State.CONTINUE, resultWithInterval.state());
    assertEquals(Long.valueOf(5000L), resultWithInterval.nextPollingDelayInMillis());
    assertTrue(resultWithInterval.artifacts().isEmpty());
    assertTrue(resultWithInterval.timeline().isEmpty());

    // Test full result constructor
    Map<String, Artifact> artifacts = new LinkedHashMap<>();
    List<TimelineEvent> timeline = new ArrayList<>();
    StepRuntime.Result fullResult =
        new StepRuntime.Result(StepRuntime.State.DONE, artifacts, timeline, 3000L);
    assertEquals(StepRuntime.State.DONE, fullResult.state());
    assertEquals(artifacts, fullResult.artifacts());
    assertEquals(timeline, fullResult.timeline());
    assertEquals(Long.valueOf(3000L), fullResult.nextPollingDelayInMillis());
  }

  @Test
  public void testResultShouldPersist() {
    // Empty results should not persist (no artifacts or timeline)
    assertFalse(StepRuntime.Result.of(StepRuntime.State.CONTINUE).shouldPersist());
    assertFalse(StepRuntime.Result.of(StepRuntime.State.DONE).shouldPersist());

    // Results with artifacts should persist
    Map<String, Artifact> artifacts = new LinkedHashMap<>();
    artifacts.put("test", new DefaultArtifact());
    assertTrue(
        new StepRuntime.Result(StepRuntime.State.CONTINUE, artifacts, new ArrayList<>(), null)
            .shouldPersist());

    // Results with timeline should persist
    List<TimelineEvent> timeline = new ArrayList<>();
    timeline.add(TimelineLogEvent.info("test"));
    assertTrue(
        new StepRuntime.Result(StepRuntime.State.DONE, new LinkedHashMap<>(), timeline, null)
            .shouldPersist());

    // Results with both should persist
    assertTrue(
        new StepRuntime.Result(StepRuntime.State.DONE, artifacts, timeline, null).shouldPersist());
  }

  @Test
  public void testResultWithNullPollingInterval() {
    // Test constructor with null polling interval
    StepRuntime.Result result =
        new StepRuntime.Result(
            StepRuntime.State.CONTINUE, new LinkedHashMap<>(), new ArrayList<>(), null);
    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertNull(result.nextPollingDelayInMillis());

    // Test 3-arg constructor (no polling interval)
    result =
        new StepRuntime.Result(StepRuntime.State.DONE, new LinkedHashMap<>(), new ArrayList<>());
    assertEquals(StepRuntime.State.DONE, result.state());
    assertNull(result.nextPollingDelayInMillis());
  }
}
