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
package com.netflix.maestro.models.definition.alerting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.Alerting;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import lombok.Data;

/** Workflow {@link Alerting} configurations per alert type. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public class AlertingTypeConfig implements Serializable {

  private static final long serialVersionUID = 4743648896393506687L;

  private Set<String> emails;
  private Set<String> pagerduties;

  @JsonProperty("slack")
  private Alerting.SlackConfig slackConfig;

  @JsonIgnore private Set<Action> actions;
  private boolean disabled;
  private Granularity granularity;

  private Integer durationMinutes;

  /** Serialize {@link Action} collection. */
  @JsonGetter("actions")
  public Set<String> serializeActions() {
    if (actions == null || actions.isEmpty()) {
      return null;
    }
    final Set<String> ret = new HashSet<>();
    actions.forEach(a -> ret.add(a.name().toLowerCase(Locale.US)));
    return ret;
  }

  /**
   * Deserialize action collection into {@link Action} enum.
   *
   * @param actionsStr a set of action strings
   */
  @JsonSetter("actions")
  public void deserializeActions(final Set<String> actionsStr) {
    if (actionsStr != null && !actionsStr.isEmpty()) {
      actions = EnumSet.noneOf(Action.class);
      actionsStr.forEach(s -> actions.add(Action.valueOf(s.toUpperCase(Locale.US))));
    }
  }

  /** Supported actions. */
  public enum Action {
    /** email action, to send alert via email. */
    EMAIL,

    /** page action, to send alert via pagerduty. */
    PAGE,

    /** slack action, to send alert via slack. */
    SLACK,

    /** cancel action, stop a workflow run. */
    CANCEL
  }

  /** Granularity of alerts. */
  public enum Granularity {
    /** Alerted at workflow level only. */
    WORKFLOW,

    /** Alerted at step level only. */
    STEP,

    /** Alerted at both workflow level and step level. */
    ALL;

    /** JSON creator. */
    @JsonCreator
    public static Granularity create(String granularity) {
      return Granularity.valueOf(granularity.toUpperCase(Locale.US));
    }
  }
}
