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
package com.netflix.maestro.engine.messageprocessors;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.processors.StartWorkflowJobProcessor;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Defaults;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class StartWorkflowJobProcessorTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowDao workflowDao;
  @Mock private MaestroRunStrategyDao runStrategyDao;

  private final String workflowId = "sample-minimal-wf";
  private StartWorkflowJobProcessor processor;
  private StartWorkflowJobEvent jobEvent;

  @Before
  public void before() throws Exception {
    processor = new StartWorkflowJobProcessor(workflowDao, runStrategyDao);
    jobEvent = StartWorkflowJobEvent.create(workflowId);
    when(runStrategyDao.dequeueWithRunStrategy(workflowId, Defaults.DEFAULT_RUN_STRATEGY))
        .thenReturn(1);
  }

  @Test
  public void testStartWorkflowInstance() {
    when(workflowDao.getRunStrategy(workflowId)).thenReturn(Defaults.DEFAULT_RUN_STRATEGY);
    processor.process(() -> jobEvent);
    verify(workflowDao, times(1)).getRunStrategy(workflowId);
    verify(runStrategyDao, times(1))
        .dequeueWithRunStrategy(workflowId, Defaults.DEFAULT_RUN_STRATEGY);
  }

  @Test
  public void testRetryableError() {
    when(workflowDao.getRunStrategy(workflowId)).thenThrow(new MaestroRetryableError("test"));
    AssertHelper.assertThrows(
        "Will retry except MaestroRetryableError",
        MaestroRetryableError.class,
        "test",
        () -> processor.process(() -> jobEvent));
  }

  @Test
  public void testNonRetryableError() {
    when(workflowDao.getRunStrategy(workflowId)).thenThrow(new MaestroNotFoundException("test"));
    processor.process(() -> jobEvent); // no error
  }

  @Test
  public void testUnexpectedError() {
    when(workflowDao.getRunStrategy(workflowId)).thenThrow(new IllegalArgumentException("test"));
    AssertHelper.assertThrows(
        "Will retry unexpected error",
        MaestroRetryableError.class,
        "Failed to start a workflow instance and will retry to run it",
        () -> processor.process(() -> jobEvent));
  }
}
