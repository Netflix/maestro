/*
 * Copyright 2026 Netflix, Inc.
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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Supported alerting actions, shared between the general and per-type alerting config. */
public enum Action {
  /** email action, to send alert via email. */
  EMAIL,

  /** page action, to send alert via pagerduty. */
  PAGE,

  /** slack action, to send alert via slack. */
  SLACK,

  /** cancel action, stop a workflow run. */
  CANCEL;

  /** Serialize an {@link Action} collection into lowercase strings. */
  public static Set<String> serialize(final Set<Action> actions) {
    if (actions == null || actions.isEmpty()) {
      return null;
    }
    final Set<String> ret = new HashSet<>();
    actions.forEach(a -> ret.add(a.name().toLowerCase(Locale.US)));
    return ret;
  }

  /** Deserialize a collection of strings into {@link Action} enum values. */
  public static Set<Action> deserialize(final Set<String> actionsStr) {
    if (actionsStr == null || actionsStr.isEmpty()) {
      return null;
    }
    final Set<Action> ret = EnumSet.noneOf(Action.class);
    actionsStr.forEach(s -> ret.add(Action.valueOf(s.toUpperCase(Locale.US))));
    return ret;
  }
}
