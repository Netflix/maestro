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
package com.netflix.maestro.engine.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.events.MaestroEvent;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Set;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;

/**
 * Webhook notification publisher implementation for MaestroNotificationPublisher. It POSTs the
 * JSON-serialized {@link MaestroEvent} to the configured endpoint whenever a step or workflow
 * status (or workflow definition) changes.
 *
 * <p>Delivery semantics follow the other publishers: a non-2xx client error (4xx) is thrown as
 * non-retryable ({@link MaestroUnprocessableEntityException}) since the receiver rejected the
 * payload, while server errors (5xx) and I/O failures are thrown as {@link MaestroRetryableError}
 * so the notification job event is retried by the queue system.
 *
 * <p>When a signing secret is configured, the request carries an {@code X-Maestro-Signature-256}
 * header with the hex-encoded HMAC-SHA256 of the request body (GitHub-webhook style, {@code
 * sha256=...}), so receivers can authenticate the sender.
 */
@Slf4j
public class WebhookNotificationPublisher implements MaestroNotificationPublisher {
  private static final String EVENT_TYPE_HEADER = "X-Maestro-Event-Type";
  private static final String SIGNATURE_HEADER = "X-Maestro-Signature-256";
  private static final String SIGNATURE_PREFIX = "sha256=";
  private static final String HMAC_ALGORITHM = "HmacSHA256";

  private final HttpClient httpClient;
  private final String url;
  private final Set<MaestroEvent.Type> eventTypes;
  private final Duration requestTimeout;
  private final SecretKeySpec signingKey;
  private final ObjectMapper objectMapper;

  /**
   * Constructor.
   *
   * @param httpClient http client used to send webhook requests
   * @param url webhook endpoint to POST events to
   * @param eventTypes event types to publish; an empty set means all event types
   * @param requestTimeout per-request timeout
   * @param signingSecret secret for the HMAC-SHA256 signature header; null or empty disables it
   * @param objectMapper object mapper to serialize events
   */
  public WebhookNotificationPublisher(
      HttpClient httpClient,
      String url,
      Set<MaestroEvent.Type> eventTypes,
      Duration requestTimeout,
      String signingSecret,
      ObjectMapper objectMapper) {
    this.httpClient = httpClient;
    this.url = url;
    this.eventTypes =
        eventTypes == null || eventTypes.isEmpty()
            ? EnumSet.allOf(MaestroEvent.Type.class)
            : EnumSet.copyOf(eventTypes);
    this.requestTimeout = requestTimeout;
    this.signingKey =
        signingSecret == null || signingSecret.isEmpty()
            ? null
            : new SecretKeySpec(signingSecret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
    this.objectMapper = objectMapper;
  }

  /** Send a Maestro event to the webhook endpoint. */
  @Override
  public void send(MaestroEvent event) {
    if (!this.eventTypes.contains(event.getType())) {
      LOG.debug("Skipping maestro event of type [{}]: not in the configured set", event.getType());
      return;
    }

    byte[] payload;
    try {
      payload = this.objectMapper.writeValueAsBytes(event);
    } catch (JsonProcessingException je) {
      throw new MaestroUnprocessableEntityException("cannot serialize maestro event: " + event, je);
    }

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder(URI.create(this.url))
            .timeout(this.requestTimeout)
            .header("Content-Type", "application/json")
            .header(EVENT_TYPE_HEADER, event.getType().name())
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
    if (this.signingKey != null) {
      requestBuilder.header(SIGNATURE_HEADER, SIGNATURE_PREFIX + sign(payload));
    }

    HttpResponse<String> response;
    try {
      response = this.httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new MaestroRetryableError(
          e, "Failed to POST a maestro event to webhook [%s] and will retry.", this.url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MaestroRetryableError(
          e, "Interrupted while sending a maestro event to webhook [%s] and will retry.", this.url);
    }

    int statusCode = response.statusCode();
    if (statusCode >= 200 && statusCode < 300) {
      LOG.info(
          "Published a maestro event of type [{}] to webhook [{}] with status code: [{}]",
          event.getType(),
          this.url,
          statusCode);
    } else if (statusCode >= 400 && statusCode < 500) {
      // The receiver actively rejected the payload: retrying the same request will not help.
      throw new MaestroUnprocessableEntityException(
          "Webhook [%s] rejected a maestro event of type [%s] with status code [%s] and body [%s]",
          this.url, event.getType(), statusCode, response.body());
    } else {
      throw new MaestroRetryableError(
          "Webhook [%s] returned status code [%s] for a maestro event of type [%s] and will retry.",
          this.url, statusCode, event.getType());
    }
  }

  private String sign(byte[] payload) {
    try {
      Mac mac = Mac.getInstance(HMAC_ALGORITHM);
      mac.init(this.signingKey);
      return HexFormat.of().formatHex(mac.doFinal(payload));
    } catch (Exception e) {
      throw new MaestroInternalError(e, "Failed to compute the webhook signature");
    }
  }
}
