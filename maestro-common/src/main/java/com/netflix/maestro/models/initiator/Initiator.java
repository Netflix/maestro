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
package com.netflix.maestro.models.initiator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.timeline.TimelineEvent;
import lombok.Getter;

/** Initiator interface for a workflow instance. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "MANUAL", value = ManualInitiator.class),
  @JsonSubTypes.Type(name = "SUBWORKFLOW", value = SubworkflowInitiator.class),
  @JsonSubTypes.Type(name = "FOREACH", value = ForeachInitiator.class),
  @JsonSubTypes.Type(name = "TEMPLATE", value = TemplateInitiator.class),
  @JsonSubTypes.Type(name = "TIME", value = TimeInitiator.class),
  @JsonSubTypes.Type(name = "SIGNAL", value = SignalInitiator.class)
})
public interface Initiator {

  /** the depth from the root to this instance. */
  int getDepth();

  /** Get initiator type info. */
  Type getType();

  /**
   * Setting a username for caller.
   *
   * @param caller
   */
  default void setCaller(User caller) {
    // no need to set caller manually by default
  }

  /** Supported Initiator types. */
  @Getter
  enum Type {
    /** Initiator for workflow instances initiated by a user manually. */
    MANUAL(true, false),
    /** Initiator for workflow instances initiated by subworkflow step. */
    SUBWORKFLOW(false, false),
    /** Initiator for workflow instances initiated by foreach step. */
    FOREACH(false, true),
    /** Initiator for workflow instances initiated by template step. */
    TEMPLATE(false, true),
    /** Initiator for workflow instances initiated by a time trigger. */
    TIME(true, false),
    /** Initiator for workflow instances initiated by signal trigger. */
    SIGNAL(true, false);

    @JsonIgnore private final boolean restartable;

    @JsonIgnore private final boolean inline;

    Type(boolean restartable, boolean inline) {
      this.restartable = restartable;
      this.inline = inline;
    }
  }

  /** Get a timeline info with initiator identity. */
  @JsonIgnore
  TimelineEvent getTimelineEvent();
}
