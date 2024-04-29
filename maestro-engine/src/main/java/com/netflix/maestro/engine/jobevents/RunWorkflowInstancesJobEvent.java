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
package com.netflix.maestro.engine.jobevents;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.engine.db.InstanceRunUuid;
import com.netflix.maestro.utils.HashHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Maestro internal job event to run a batch of workflow instances for a given workflow id. It can
 * be used to run a single workflow instance as well.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "instance_run_uuids"},
    alphabetic = true)
@Data
public class RunWorkflowInstancesJobEvent implements MaestroJobEvent {
  @Valid @NotNull private String workflowId;

  @Valid @NotEmpty private List<InstanceRunUuid> instanceRunUuids;

  /** Static method to initialize a StartWorkflowInstancesJob. */
  public static RunWorkflowInstancesJobEvent init(String workflowId) {
    RunWorkflowInstancesJobEvent job = new RunWorkflowInstancesJobEvent();
    job.workflowId = workflowId;
    job.instanceRunUuids = new ArrayList<>();
    return job;
  }

  /** Add a workflow run (instance id, run id, uuid) to the job event. */
  @JsonIgnore
  public void addOneRun(long instanceId, long runId, String uuid) {
    addOneRun(new InstanceRunUuid(instanceId, runId, uuid));
  }

  /** Add a workflow run (instance id, run id, uuid) to the job event. */
  @JsonIgnore
  public void addOneRun(InstanceRunUuid iru) {
    this.instanceRunUuids.add(iru);
  }

  /** Return the size of instances. */
  @JsonIgnore
  public int size() {
    return instanceRunUuids == null ? 0 : instanceRunUuids.size();
  }

  /** Convert instanceRunUuids to a stream of job events with a single instanceRunUuid. */
  @JsonIgnore
  public Stream<RunWorkflowInstancesJobEvent> singletonStream() {
    return instanceRunUuids.stream()
        .map(
            instanceRunUuid -> {
              RunWorkflowInstancesJobEvent jobEvent = new RunWorkflowInstancesJobEvent();
              jobEvent.workflowId = workflowId;
              jobEvent.instanceRunUuids = Collections.singletonList(instanceRunUuid);
              return jobEvent;
            });
  }

  @Override
  public Type getType() {
    return Type.RUN_WORKFLOW_INSTANCES_JOB_EVENT;
  }

  /** use MD5 of the job event as the message key for dedup. */
  @JsonIgnore
  @Override
  public String getMessageKey() {
    return HashHelper.md5(toString());
  }
}
