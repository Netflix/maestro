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
package com.netflix.maestro.server.properties;

import com.netflix.maestro.models.events.MaestroEvent;
import java.util.Collections;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Webhook notifier config properties, used when {@code maestro.notifier.type} is {@code webhook}.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "maestro.notifier.webhook")
public class WebhookNotifierProperties {
  private static final long DEFAULT_REQUEST_TIMEOUT_MS = 10_000L;

  /** Webhook endpoint URL to POST maestro events to. Required. */
  private String url;

  /** Event types to publish. An empty set publishes all event types. */
  private Set<MaestroEvent.Type> eventTypes = Collections.emptySet();

  /** Per-request timeout in milliseconds. */
  private long requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

  /**
   * Secret for the HMAC-SHA256 signature header ({@code X-Maestro-Signature-256}). Null or empty
   * disables signing.
   */
  private String signingSecret;
}
