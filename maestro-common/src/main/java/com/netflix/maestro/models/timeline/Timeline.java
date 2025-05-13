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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.Data;

/**
 * Runtime timeline, it includes change events happened at runtime. It will automatically trim the
 * timeline events if the size of the events is larger than {@link Timeline#TIMELINE_SIZE_LIMIT}.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public final class Timeline {
  static final int TIMELINE_SIZE_LIMIT = 32;

  @JsonValue @NotNull @Valid private final List<TimelineEvent> timelineEvents;

  /** timeline constructor. */
  @JsonCreator
  public Timeline(List<TimelineEvent> events) {
    timelineEvents = new ArrayList<>();
    if (events != null) {
      addAll(events);
    }
  }

  /**
   * Add a new change event if it is not identical to the latest one.
   *
   * @param event the event to add
   * @return true if timelineEvents is changed, otherwise false.
   */
  @JsonIgnore
  public boolean add(TimelineEvent event) {
    if (latestMatched(event)) {
      return false;
    }
    if (timelineEvents.size() == TIMELINE_SIZE_LIMIT) {
      timelineEvents.remove(0);
    }
    return timelineEvents.add(event);
  }

  /**
   * Add a list of new change events if they are not identical to the latest one.
   *
   * @param events events to add
   * @return true if timelineEvents is changed, otherwise false.
   */
  @JsonIgnore
  public boolean addAll(Collection<TimelineEvent> events) {
    if (events == null || events.isEmpty()) {
      return false;
    }
    boolean ret =
        events.stream()
            .map(
                event -> {
                  if (latestMatched(event)) {
                    return false;
                  }
                  return timelineEvents.add(event);
                })
            .reduce(false, Boolean::logicalOr);
    if (timelineEvents.size() > TIMELINE_SIZE_LIMIT) {
      timelineEvents.subList(0, timelineEvents.size() - TIMELINE_SIZE_LIMIT).clear();
    }
    return ret;
  }

  /** If there is any change event. */
  @JsonIgnore
  public boolean isEmpty() {
    return timelineEvents.isEmpty();
  }

  /** check if the latest event in the list matches the input event. */
  @JsonIgnore
  @VisibleForTesting
  boolean latestMatched(TimelineEvent event) {
    if (timelineEvents == null || timelineEvents.isEmpty()) {
      return false;
    }
    return timelineEvents.get(timelineEvents.size() - 1).isIdentical(event);
  }

  /**
   * Enrich the current workflow timeline by adding additional events from workflow instance for
   * external API.
   */
  @JsonIgnore
  public void enrich(WorkflowInstance instance) {
    if (instance.getCreateTime() != null) {
      timelineEvents.add(
          TimelineStatusEvent.create(instance.getCreateTime(), WorkflowInstance.Status.CREATED));
    }
    if (instance.getStartTime() != null) {
      timelineEvents.add(
          TimelineStatusEvent.create(instance.getStartTime(), WorkflowInstance.Status.IN_PROGRESS));
    }
    if (instance.getEndTime() != null) {
      timelineEvents.add(TimelineStatusEvent.create(instance.getEndTime(), instance.getStatus()));
    }
    Collections.sort(timelineEvents);
  }

  /**
   * Enrich the current step timeline by adding additional events from step instance for external
   * API.
   */
  @JsonIgnore
  public void enrich(StepInstance instance) {
    StepRuntimeState runtimeState = instance.getRuntimeState();
    if (runtimeState != null) {
      if (runtimeState.getCreateTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(runtimeState.getCreateTime(), StepInstance.Status.CREATED));
      }
      if (runtimeState.getInitializeTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(
                runtimeState.getInitializeTime(), StepInstance.Status.INITIALIZED));
      }
      if (runtimeState.getPauseTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(runtimeState.getPauseTime(), StepInstance.Status.PAUSED));
      }
      if (runtimeState.getWaitSignalTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(
                runtimeState.getWaitSignalTime(), StepInstance.Status.WAITING_FOR_SIGNALS));
      }
      if (runtimeState.getEvaluateParamTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(
                runtimeState.getEvaluateParamTime(), StepInstance.Status.EVALUATING_PARAMS));
      }
      if (runtimeState.getWaitPermitTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(
                runtimeState.getWaitPermitTime(), StepInstance.Status.WAITING_FOR_PERMITS));
      }
      if (runtimeState.getStartTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(runtimeState.getStartTime(), StepInstance.Status.STARTING));
      }
      if (runtimeState.getExecuteTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(runtimeState.getExecuteTime(), StepInstance.Status.RUNNING));
      }
      if (runtimeState.getFinishTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(
                runtimeState.getFinishTime(), StepInstance.Status.FINISHING));
      }
      if (runtimeState.getEndTime() != null) {
        timelineEvents.add(
            TimelineStatusEvent.create(runtimeState.getEndTime(), runtimeState.getStatus()));
      }
      if (instance.getSignalDependencies() != null
          && instance.getSignalDependencies().getInfo() != null) {
        timelineEvents.add(instance.getSignalDependencies().getInfo());
      }
      if (instance.getSignalOutputs() != null && instance.getSignalOutputs().getInfo() != null) {
        timelineEvents.add(instance.getSignalOutputs().getInfo());
      }
      if (instance.getArtifacts() != null) {
        if (instance.getArtifacts().containsKey(Artifact.Type.TITUS.key())) {
          timelineEvents.addAll(
              instance.getArtifacts().get(Artifact.Type.TITUS.key()).asTitus().getMilestones());
        }
      }
    }
    Collections.sort(timelineEvents);
  }
}
