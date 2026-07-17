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
package com.netflix.maestro.flow.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Payload carried by a message to a task. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    defaultImpl = DefaultMessagePayload.class)
@JsonSubTypes({@JsonSubTypes.Type(name = "DEFAULT", value = DefaultMessagePayload.class)})
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface MessagePayload {
  /** empty payload used when a message carries no data. */
  MessagePayload DEFAULT = new DefaultMessagePayload();

  /** Get message payload type info. */
  Type getType();

  /** supported message payload types. */
  enum Type {
    /** default empty payload. */
    DEFAULT
  }
}
