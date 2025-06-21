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
package com.netflix.maestro.engine.processors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class StartWorkflowJobEventProcessorTest extends MaestroEngineBaseTest {
  private static final String WORKFLOW_ID = "sample-minimal-wf";

  @Mock private MaestroWorkflowDao workflowDao;
  @Mock private MaestroRunStrategyDao runStrategyDao;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private WorkflowRunner workflowRunner;
  @Mock private WorkflowInstance instance;

  private StartWorkflowJobEventProcessor processor;
  private StartWorkflowJobEvent jobEvent;

  @Before
  public void before() throws Exception {
    processor =
        new StartWorkflowJobEventProcessor(
            workflowDao, runStrategyDao, instanceDao, workflowRunner);
    jobEvent = StartWorkflowJobEvent.create(WORKFLOW_ID);
    when(runStrategyDao.dequeueWithRunStrategy(WORKFLOW_ID, Defaults.DEFAULT_RUN_STRATEGY))
        .thenReturn(List.of(new InstanceRunUuid(1, 2, "uuid1")));
  }

  @Test
  public void testStartWorkflowInstance() {
    when(workflowDao.getRunStrategy(WORKFLOW_ID)).thenReturn(Defaults.DEFAULT_RUN_STRATEGY);
    when(instanceDao.getWorkflowInstanceRun(WORKFLOW_ID, 1, 2)).thenReturn(instance);
    Assert.assertTrue(processor.process(jobEvent).isEmpty());
    verify(workflowDao, times(1)).getRunStrategy(WORKFLOW_ID);
    verify(runStrategyDao, times(1))
        .dequeueWithRunStrategy(WORKFLOW_ID, Defaults.DEFAULT_RUN_STRATEGY);
    verify(instanceDao, times(1)).getWorkflowInstanceRun(WORKFLOW_ID, 1, 2);
    verify(workflowRunner, times(1)).run(instance, "uuid1");
  }

  @Test
  public void testStartWorkflowInstanceOverDequeueLimit() {
    when(workflowDao.getRunStrategy(WORKFLOW_ID)).thenReturn(Defaults.DEFAULT_RUN_STRATEGY);
    List<InstanceRunUuid> uuids = new ArrayList<>();
    for (int i = 0; i < Constants.DEQUEUE_SIZE_LIMIT; i++) {
      uuids.add(new InstanceRunUuid(1, 2, "uuid" + i));
    }
    when(runStrategyDao.dequeueWithRunStrategy(WORKFLOW_ID, Defaults.DEFAULT_RUN_STRATEGY))
        .thenReturn(uuids);
    when(instanceDao.getWorkflowInstanceRun(WORKFLOW_ID, 1, 2)).thenReturn(instance);

    AssertHelper.assertThrows(
        "Will dequeue again if over dequeue limit",
        MaestroRetryableError.class,
        "Hit DEQUEUE_SIZE_LIMIT for workflow",
        () -> processor.process(jobEvent));

    verify(workflowDao, times(1)).getRunStrategy(WORKFLOW_ID);
    verify(runStrategyDao, times(1))
        .dequeueWithRunStrategy(WORKFLOW_ID, Defaults.DEFAULT_RUN_STRATEGY);
    verify(instanceDao, times(uuids.size())).getWorkflowInstanceRun(WORKFLOW_ID, 1, 2);
    verify(workflowRunner, times(uuids.size())).run(eq(instance), anyString());
  }

  @Test
  public void testRetryableError() {
    when(workflowDao.getRunStrategy(WORKFLOW_ID)).thenThrow(new MaestroRetryableError("test"));
    AssertHelper.assertThrows(
        "Will retry except MaestroRetryableError",
        MaestroRetryableError.class,
        "test",
        () -> processor.process(jobEvent));
  }

  @Test
  public void testNonRetryableError() {
    when(workflowDao.getRunStrategy(WORKFLOW_ID)).thenThrow(new MaestroNotFoundException("test"));
    processor.process(jobEvent); // no error
  }

  @Test
  public void testUnexpectedError() {
    when(workflowDao.getRunStrategy(WORKFLOW_ID)).thenThrow(new IllegalArgumentException("test"));
    AssertHelper.assertThrows(
        "Will retry unexpected error",
        MaestroRetryableError.class,
        "Failed to start a workflow instance and will retry to run it",
        () -> processor.process(jobEvent));
  }
}
