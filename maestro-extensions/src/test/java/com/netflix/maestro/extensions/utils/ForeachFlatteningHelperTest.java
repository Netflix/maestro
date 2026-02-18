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
package com.netflix.maestro.extensions.utils;

import static com.netflix.maestro.extensions.utils.ForeachFlatteningHelper.getIterationRank;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator.Info;
import com.netflix.maestro.models.instance.StepInstance;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ForeachFlatteningHelperTest extends MaestroBaseTest {

  private static final long WORKFLOW_INSTANCE_ID = 2;
  private static final long WORKFLOW_INSTANCE_ID_LONG = 265;
  private static final long WORKFLOW_INSTANCE_ID_LONG_NON_DIGIT_ENCODING = 265000009998L;
  private static final long NESTED_INSTANCE_ID = 5;
  private static final long NESTED_INSTANCE_ID_LONG = 5678;
  private static final long NESTED_WORKFLOW_RUN_ID = 33456000999898L;
  private static final long NESTED_FOREACH_STEP_ATTEMPT_ID = 2;
  private static final long ROOT_WORKFLOW_RUN_ID = 2;
  private static final long ROOT_FOREACH_STEP_ATTEMPT_ID = 1;
  private static final ForeachFlatteningHelper FOREACH_FLATTENING_HELPER =
      new ForeachFlatteningHelper();

  @Mock private StepInstance stepInstance;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testNestedIterationRankWithSingleDigits() {
    UpstreamInitiator initiator = getUpstreamInitiator(NESTED_INSTANCE_ID);
    String iterationRank =
        FOREACH_FLATTENING_HELPER.getIterationRank(initiator, WORKFLOW_INSTANCE_ID);
    String expectedIterationRank =
        String.format("1%d-1%d", NESTED_INSTANCE_ID, WORKFLOW_INSTANCE_ID);
    assertEquals(expectedIterationRank, iterationRank);
  }

  @Test
  public void testNestedIterationRankWithMoreThanOneDigits() {
    UpstreamInitiator initiator = getUpstreamInitiator(NESTED_INSTANCE_ID_LONG);
    String iterationRank =
        FOREACH_FLATTENING_HELPER.getIterationRank(initiator, WORKFLOW_INSTANCE_ID_LONG);
    String expectedIterationRank =
        String.format("4%d-3%d", NESTED_INSTANCE_ID_LONG, WORKFLOW_INSTANCE_ID_LONG);
    assertEquals(expectedIterationRank, iterationRank);
  }

  @Test
  public void testNestedIterationRankWithNonDigitEncoding() {
    UpstreamInitiator initiator = getUpstreamInitiator(NESTED_INSTANCE_ID_LONG);
    String iterationRank =
        FOREACH_FLATTENING_HELPER.getIterationRank(
            initiator, WORKFLOW_INSTANCE_ID_LONG_NON_DIGIT_ENCODING);
    String expectedIterationRank =
        String.format(
            "4%d-C%d", NESTED_INSTANCE_ID_LONG, WORKFLOW_INSTANCE_ID_LONG_NON_DIGIT_ENCODING);
    assertEquals(expectedIterationRank, iterationRank);
  }

  @Test
  public void testGetAttemptSequenceWithDigitsAndNonDigitEncoding() {
    UpstreamInitiator initiator = getUpstreamInitiator(NESTED_INSTANCE_ID_LONG);
    long immediateInlineWorkflowRunId = 8L;
    when(stepInstance.getWorkflowRunId()).thenReturn(immediateInlineWorkflowRunId);
    long leafStepAttemptId = 1L;
    when(stepInstance.getStepAttemptId()).thenReturn(leafStepAttemptId);
    String attemptSeq = FOREACH_FLATTENING_HELPER.getAttemptSeq(initiator, stepInstance);
    String expectedAttemptSeq =
        String.format(
            "1%d-1%d-E%d-1%d-1%d-1%d",
            ROOT_WORKFLOW_RUN_ID,
            ROOT_FOREACH_STEP_ATTEMPT_ID,
            NESTED_WORKFLOW_RUN_ID,
            NESTED_FOREACH_STEP_ATTEMPT_ID,
            immediateInlineWorkflowRunId,
            leafStepAttemptId);
    assertEquals(expectedAttemptSeq, attemptSeq);
  }

  @Test
  public void testGetIterationRankFromIterationId() {
    assertNull(getIterationRank(null));
    assertNull(getIterationRank(""));
    assertEquals("11", getIterationRank("1"));
    assertEquals("11-11", getIterationRank("1-1"));
    assertEquals("221-13", getIterationRank("21-3"));
    assertEquals("14-17-222", getIterationRank("4-7-22"));
  }

  private static UpstreamInitiator getUpstreamInitiator(long nestedInstanceIdLong) {
    UpstreamInitiator initiator = new ForeachInitiator();
    Info rootInfo = new Info();
    String rootWorkflowId = "root-workflow-id";
    rootInfo.setWorkflowId(rootWorkflowId);
    String rootStepId = "root_step-id";
    rootInfo.setStepId(rootStepId);
    int rootWorkflowInstanceId = 10;
    rootInfo.setInstanceId(rootWorkflowInstanceId);
    rootInfo.setRunId(ROOT_WORKFLOW_RUN_ID);
    rootInfo.setStepAttemptId(ROOT_FOREACH_STEP_ATTEMPT_ID);

    Info nestedInfo = new Info();
    nestedInfo.setWorkflowId("maestro_foreach_nested_inline_id");
    nestedInfo.setStepId("nested-step-id");

    nestedInfo.setInstanceId(nestedInstanceIdLong);

    nestedInfo.setRunId(NESTED_WORKFLOW_RUN_ID);
    nestedInfo.setStepAttemptId(NESTED_FOREACH_STEP_ATTEMPT_ID);

    List<Info> ancestors = new ArrayList<>();
    ancestors.add(rootInfo);
    ancestors.add(nestedInfo);
    initiator.setAncestors(ancestors);
    return initiator;
  }
}
