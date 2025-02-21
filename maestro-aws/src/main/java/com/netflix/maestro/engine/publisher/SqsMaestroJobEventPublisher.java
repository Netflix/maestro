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

import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent.Type;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import java.util.Optional;

public class SqsMaestroJobEventPublisher extends SqsAbstractEventPublisher {
  private final SqsProperties props;

  public SqsMaestroJobEventPublisher(
      AmazonSQS amazonSqs,
      ObjectMapper objectMapper,
      SqsProperties sqsProperties,
      MaestroMetrics metrics) {
    super(amazonSqs, objectMapper, metrics);
    this.props = sqsProperties;
  }

  @Override
  protected Optional<Details> detailsToPublishOnException(Exception ex) {
    return Optional.of(
        Details.create(ex, false, "Failed to process a MaestroJobEvent while publishing to SQS"));
  }

  @Override
  protected String getQueueUrlToPublish(MaestroJobEvent event) {
    if (event.getType() == Type.START_WORKFLOW_JOB_EVENT) {
      return props.getStartWorkflowJobQueueUrl();
    } else if (event.getType() == Type.RUN_WORKFLOW_INSTANCES_JOB_EVENT) {
      return props.getRunWorkflowInstancesJobQueueUrl();
    } else if (event.getType() == Type.TERMINATE_INSTANCES_JOB_EVENT) {
      return props.getTerminateInstancesJobQueueUrl();
    } else if (event.getType() == Type.TERMINATE_THEN_RUN_JOB_EVENT) {
      return props.getTerminateThenRunInstanceJobQueueUrl();
    } else if (event.getType() == Type.STEP_INSTANCE_UPDATE_JOB_EVENT
        || event.getType() == Type.WORKFLOW_INSTANCE_UPDATE_JOB_EVENT
        || event.getType() == Type.WORKFLOW_VERSION_UPDATE_JOB_EVENT) {
      return props.getPublishJobQueueUrl();
    } else if (event.getType() == Type.DELETE_WORKFLOW_JOB_EVENT) {
      return props.getDeleteWorkflowJobQueueUrl();
    } else if (event.getType() == Type.STEP_INSTANCE_WAKE_UP_JOB_EVENT) {
      return props.getStepWakeUpJobQueueUrl();
    } else {
      throw new MaestroInternalError(
          "The job type: [%s] does not have a corresponding SQS queue configured",
          event.getType().name());
    }
  }

  @Override
  protected Optional<String> uniqueKeyForFifoDeduplication(
      MaestroJobEvent event, String messageBody) {
    if (event.getType() == Type.RUN_WORKFLOW_INSTANCES_JOB_EVENT) {
      return Optional.of(event.getMessageKey());
    } else if (event.getType() == Type.STEP_INSTANCE_WAKE_UP_JOB_EVENT) {
      return Optional.of(event.getMessageKey());
    }
    return Optional.empty();
  }
}
