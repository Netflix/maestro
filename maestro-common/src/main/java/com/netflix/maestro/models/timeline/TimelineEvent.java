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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.exceptions.MaestroInternalError;

/** Timeline event to be stored in timeline. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "LOG", value = TimelineLogEvent.class),
  @JsonSubTypes.Type(name = "DETAILS", value = TimelineDetailsEvent.class),
  @JsonSubTypes.Type(name = "STATUS", value = TimelineStatusEvent.class),
  @JsonSubTypes.Type(name = "ACTION", value = TimelineActionEvent.class),
  @JsonSubTypes.Type(name = "MILESTONE", value = TimelineMilestoneEvent.class)
})
public interface TimelineEvent extends Comparable<TimelineEvent> {

  /** Get timeline event timestamp. */
  Long getTimestamp();

  /** Get timeline event type. */
  Type getType();

  /** Get short timeline message. */
  String getMessage();

  /** Check if this event is identical to the input event. */
  @JsonIgnore
  boolean isIdentical(TimelineEvent event);

  /** By default, the event is ordered by its timestamp. */
  @Override
  default int compareTo(TimelineEvent o) {
    return getTimestamp().compareTo(o.getTimestamp());
  }

  /** Cast timeline to {@link TimelineLogEvent} if possible. */
  default TimelineLogEvent asLog() {
    throw new MaestroInternalError(
        "TimelineEvent with type [%s] cannot be treated as LOG event", getType());
  }

  /** Cast timeline to {@link TimelineDetailsEvent} if possible. */
  default TimelineDetailsEvent asDetails() {
    throw new MaestroInternalError(
        "TimelineEvent with type [%s] cannot be treated as DETAILS event", getType());
  }

  /** Cast timeline to {@link TimelineStatusEvent} if possible. */
  default TimelineStatusEvent asStatus() {
    throw new MaestroInternalError(
        "TimelineEvent with type [%s] cannot be treated as STATUS event", getType());
  }

  /** Cast timeline to {@link TimelineStatusEvent} if possible. */
  default TimelineActionEvent asAction() {
    throw new MaestroInternalError(
        "TimelineEvent with type [%s] cannot be treated as ACTION event", getType());
  }

  /** Cast timeline to {@link TimelineMilestoneEvent} if possible. */
  default TimelineMilestoneEvent asMilestone() {
    throw new MaestroInternalError(
        "TimelineEvent with type [%s] cannot be treated as MILESTONE event", getType());
  }

  /** Supported timeline event type. */
  enum Type {
    /** LOG. */
    LOG,
    /** DETAILS. */
    DETAILS,
    /** STATUS. */
    STATUS,
    /** ACTION. */
    ACTION,
    /** MILESTONE. */
    MILESTONE
  }

  /** Supported timeline event level. */
  enum Level {
    /** TRACE. */
    TRACE,
    /** DEBUG. */
    DEBUG,
    /** INFO. */
    INFO,
    /** WARN. */
    WARN,
    /** ERROR. */
    ERROR
  }
}
