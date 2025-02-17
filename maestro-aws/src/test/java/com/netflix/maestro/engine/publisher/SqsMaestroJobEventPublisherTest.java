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
package com.netflix.maestro.engine.publisher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.db.InstanceRunUuid;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.jobevents.TerminateInstancesJobEvent;
import com.netflix.maestro.engine.jobevents.TerminateThenRunInstanceJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.utils.HashHelper;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class SqsMaestroJobEventPublisherTest extends MaestroBaseTest {
  private static final String WORKFLOW_ID = "sample-minimal-wf";
  private static final User TESTER = User.create("tester");
  private static final TerminateInstancesJobEvent TERMINATE_INSTANCES_JOB_EVENT =
      TerminateInstancesJobEvent.init(
          WORKFLOW_ID, Actions.WorkflowInstanceAction.KILL, TESTER, "test-reason");
  private static final String TERMINATE_INSTANCES_JOB_QUEUE_URL =
      "terminate-instances-job-queue-url";
  private static final String TERMINATE_THEN_RUN_INSTANCE_JOB_QUEUE_URL =
      "terminate-then-run-instance-job-queue-url";
  private static final String PUBLISH_JOB_QUEUE_URL = "publish-job-queue-url";
  private static final String START_WORKFLOW_JOB_QUEUE_URL = "start-workflow-job-queue-url";
  private static final String RUN_WORKFLOW_INSTANCES_JOB_QUEUE_URL =
      "run-workflow-instances-job-queue-url";
  private static final String DELETE_WORKFLOW_JOB_QUEUE_URL = "delete-workflow-job-queue-url";
  private static final String STEP_INSTANCE_ACTION_JOB_QUEUE_URL =
      "step-instance-action-job-queue-url";

  private SqsMaestroJobEventPublisher sqsMaestroJobEventPublisher;
  private TerminateThenRunInstanceJobEvent terminateThenRunInstanceJobEvent;
  private RunWorkflowInstancesJobEvent runWorkflowInstancesJobEvent;
  private final StepInstanceUpdateJobEvent stepInstanceUpdateJobEvent =
      new StepInstanceUpdateJobEvent();
  private final WorkflowInstanceUpdateJobEvent workflowInstanceUpdateJobEvent =
      new WorkflowInstanceUpdateJobEvent();
  private final WorkflowVersionUpdateJobEvent workflowVersionUpdateJobEvent =
      new WorkflowVersionUpdateJobEvent();
  private final StartWorkflowJobEvent startWorkflowJobEvent =
      StartWorkflowJobEvent.create(WORKFLOW_ID);
  private final DeleteWorkflowJobEvent deleteWorkflowJobEvent =
      DeleteWorkflowJobEvent.create(WORKFLOW_ID, 12345L, TESTER);
  private final StepInstanceWakeUpEvent stepInstanceWakeUpEvent = new StepInstanceWakeUpEvent();

  private AmazonSQS amazonSqs;
  private MaestroMetricRepo metricRepo;

  @Before
  public void setup() {
    amazonSqs = mock(AmazonSQS.class);
    terminateThenRunInstanceJobEvent =
        TerminateThenRunInstanceJobEvent.init(
            WORKFLOW_ID, Actions.WorkflowInstanceAction.STOP, TESTER, "test-reason");
    terminateThenRunInstanceJobEvent.addOneRun(new InstanceRunUuid(1L, 1L, "uuid1"));
    terminateThenRunInstanceJobEvent.addOneRun(new InstanceRunUuid(2L, 1L, "uuid2"));
    terminateThenRunInstanceJobEvent.addRunAfter(3L, 1L, "uuid3");

    runWorkflowInstancesJobEvent = RunWorkflowInstancesJobEvent.init(WORKFLOW_ID);
    runWorkflowInstancesJobEvent.addOneRun(1L, 1L, "uuid1");
    runWorkflowInstancesJobEvent.addOneRun(2L, 1L, "uuid2");
    runWorkflowInstancesJobEvent.addOneRun(3L, 1L, "uuid3");

    SqsProperties sqsProperties = getSqsProperties();
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    sqsMaestroJobEventPublisher =
        new SqsMaestroJobEventPublisher(amazonSqs, MAPPER, sqsProperties, metricRepo);
  }

  private static SqsProperties getSqsProperties() {
    SqsProperties sqsProperties = new SqsProperties();
    sqsProperties.setTerminateInstancesJobQueueUrl(TERMINATE_INSTANCES_JOB_QUEUE_URL);
    sqsProperties.setTerminateThenRunInstanceJobQueueUrl(TERMINATE_THEN_RUN_INSTANCE_JOB_QUEUE_URL);
    sqsProperties.setPublishJobQueueUrl(PUBLISH_JOB_QUEUE_URL);
    sqsProperties.setStartWorkflowJobQueueUrl(START_WORKFLOW_JOB_QUEUE_URL);
    sqsProperties.setRunWorkflowInstancesJobQueueUrl(RUN_WORKFLOW_INSTANCES_JOB_QUEUE_URL);
    sqsProperties.setDeleteWorkflowJobQueueUrl(DELETE_WORKFLOW_JOB_QUEUE_URL);
    sqsProperties.setStepWakeUpJobQueueUrl(STEP_INSTANCE_ACTION_JOB_QUEUE_URL);
    return sqsProperties;
  }

  @Test
  public void testTerminateInstancesJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(TERMINATE_INSTANCES_JOB_EVENT, 2000);

    String terminateInstancesJobEventStr = MAPPER.writeValueAsString(TERMINATE_INSTANCES_JOB_EVENT);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(TERMINATE_INSTANCES_JOB_QUEUE_URL)
                .withMessageBody(terminateInstancesJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                TerminateInstancesJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testTerminateInstancesJobEventPublisherException() throws Exception {
    com.amazonaws.services.sqs.model.UnsupportedOperationException unsupportedEx =
        new com.amazonaws.services.sqs.model.UnsupportedOperationException("Unsupported");
    when(amazonSqs.sendMessage(any())).thenThrow(unsupportedEx);

    Optional<Details> details =
        sqsMaestroJobEventPublisher.publish(TERMINATE_INSTANCES_JOB_EVENT, 2000);

    String terminateInstancesJobEventStr = MAPPER.writeValueAsString(TERMINATE_INSTANCES_JOB_EVENT);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(TERMINATE_INSTANCES_JOB_QUEUE_URL)
                .withMessageBody(terminateInstancesJobEventStr)
                .withDelaySeconds(2));
    assertTrue(details.isPresent());
    assertEquals(details.get().getCause(), unsupportedEx);
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_FAILURE_METRIC,
                SqsMaestroJobEventPublisher.class,
                MetricConstants.TYPE_TAG,
                UnsupportedOperationException.class.getSimpleName(),
                AwsMetricConstants.JOB_TYPE_TAG,
                TerminateInstancesJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testTerminateThenRunInstanceJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(terminateThenRunInstanceJobEvent, 2000);

    String terminateThenRunInstanceJobEventStr =
        MAPPER.writeValueAsString(terminateThenRunInstanceJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(TERMINATE_THEN_RUN_INSTANCE_JOB_QUEUE_URL)
                .withMessageBody(terminateThenRunInstanceJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                TerminateThenRunInstanceJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testStepInstanceUpdateJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(stepInstanceUpdateJobEvent, 2000);

    String stepInstanceUpdateJobEventStr = MAPPER.writeValueAsString(stepInstanceUpdateJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(PUBLISH_JOB_QUEUE_URL)
                .withMessageBody(stepInstanceUpdateJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                StepInstanceUpdateJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testWorkflowInstanceUpdateJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(workflowInstanceUpdateJobEvent, 2000);

    String workflowInstanceUpdateJobEventStr =
        MAPPER.writeValueAsString(workflowInstanceUpdateJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(PUBLISH_JOB_QUEUE_URL)
                .withMessageBody(workflowInstanceUpdateJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                WorkflowInstanceUpdateJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testWorkflowVersionUpdateJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(workflowVersionUpdateJobEvent, 2000);

    String workflowVersionUpdateJobEventStr =
        MAPPER.writeValueAsString(workflowVersionUpdateJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(PUBLISH_JOB_QUEUE_URL)
                .withMessageBody(workflowVersionUpdateJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                WorkflowVersionUpdateJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testStartWorkflowJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(startWorkflowJobEvent, 2000);

    String startWorkflowJobEventStr = MAPPER.writeValueAsString(startWorkflowJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(START_WORKFLOW_JOB_QUEUE_URL)
                .withMessageBody(startWorkflowJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                StartWorkflowJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testRunWorkflowInstancesJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(runWorkflowInstancesJobEvent, 2000);

    String runWorkflowInstancesJobEventStr =
        MAPPER.writeValueAsString(runWorkflowInstancesJobEvent);
    String expectedMessageIdForDedup = HashHelper.md5(runWorkflowInstancesJobEvent.toString());
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(RUN_WORKFLOW_INSTANCES_JOB_QUEUE_URL)
                .withMessageBody(runWorkflowInstancesJobEventStr)
                .withMessageDeduplicationId(expectedMessageIdForDedup)
                .withMessageGroupId(expectedMessageIdForDedup)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                RunWorkflowInstancesJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testDeleteWorkflowJobEventPublisher() throws Exception {
    sqsMaestroJobEventPublisher.publish(deleteWorkflowJobEvent, 2000);

    String deleteWorkflowJobEventStr = MAPPER.writeValueAsString(deleteWorkflowJobEvent);
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(DELETE_WORKFLOW_JOB_QUEUE_URL)
                .withMessageBody(deleteWorkflowJobEventStr)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                DeleteWorkflowJobEvent.class.getSimpleName())
            .count());
  }

  @Test
  public void testStepInstanceActionJobEventPublisher() throws Exception {
    String workflowId = "sample-wf-id";
    long workflowInstanceId = 2;
    long workflowRunId = 3;
    Actions.WorkflowInstanceAction workflowInstanceAction = Actions.WorkflowInstanceAction.KILL;
    stepInstanceWakeUpEvent.setWorkflowId(workflowId);
    stepInstanceWakeUpEvent.setWorkflowAction(workflowInstanceAction);
    stepInstanceWakeUpEvent.setWorkflowInstanceId(workflowInstanceId);
    stepInstanceWakeUpEvent.setWorkflowRunId(workflowRunId);
    stepInstanceWakeUpEvent.setEntityType(StepInstanceWakeUpEvent.EntityType.WORKFLOW);
    sqsMaestroJobEventPublisher.publish(stepInstanceWakeUpEvent, 2000);

    String stepInstanceWakeUpEventStr = MAPPER.writeValueAsString(stepInstanceWakeUpEvent);
    String expectedMessageIdForDedup =
        HashHelper.md5(
            String.format(
                "[%s]_[%s]_[%s]_[%s]_[%s]",
                StepInstanceWakeUpEvent.EntityType.WORKFLOW.name(),
                workflowId,
                workflowInstanceId,
                workflowRunId,
                workflowInstanceAction.name()));
    verify(amazonSqs, times(1))
        .sendMessage(
            new SendMessageRequest()
                .withQueueUrl(STEP_INSTANCE_ACTION_JOB_QUEUE_URL)
                .withMessageBody(stepInstanceWakeUpEventStr)
                .withMessageDeduplicationId(expectedMessageIdForDedup)
                .withMessageGroupId(expectedMessageIdForDedup)
                .withDelaySeconds(2));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
                SqsMaestroJobEventPublisher.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                StepInstanceWakeUpEvent.class.getSimpleName())
            .count());
  }
}
