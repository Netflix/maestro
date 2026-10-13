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

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.WorkflowVersionChangeEvent;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WebhookNotificationPublisherTest extends MaestroBaseTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final String SECRET = "test-secret";

  private HttpServer server;
  private String url;
  private final AtomicInteger statusToReturn = new AtomicInteger(200);
  private final AtomicInteger requestCount = new AtomicInteger();
  private final AtomicReference<byte[]> lastBody = new AtomicReference<>();
  private final Map<String, String> lastHeaders = new ConcurrentHashMap<>();

  @Before
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/webhook",
        exchange -> {
          requestCount.incrementAndGet();
          lastBody.set(exchange.getRequestBody().readAllBytes());
          exchange
              .getRequestHeaders()
              .forEach((key, values) -> lastHeaders.put(key, values.getFirst()));
          exchange.sendResponseHeaders(statusToReturn.get(), -1);
          exchange.close();
        });
    server.start();
    url = "http://localhost:" + server.getAddress().getPort() + "/webhook";
  }

  @After
  public void tearDown() {
    server.stop(0);
  }

  @Test
  public void testPublishEventWithSignature() throws Exception {
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(), url, Collections.emptySet(), TIMEOUT, SECRET, MAPPER);
    MaestroEvent event = sampleEvent();

    publisher.send(event);

    Assert.assertEquals(1, requestCount.get());
    Assert.assertEquals("application/json", lastHeaders.get("Content-type"));
    Assert.assertEquals(
        MaestroEvent.Type.WORKFLOW_VERSION_CHANGE_EVENT.name(),
        lastHeaders.get("X-maestro-event-type"));
    // the body round-trips as the same event
    Assert.assertEquals(
        MAPPER.readTree(MAPPER.writeValueAsBytes(event)), MAPPER.readTree(lastBody.get()));
    // the signature is the HMAC-SHA256 of the exact bytes received
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
    Assert.assertEquals(
        "sha256=" + HexFormat.of().formatHex(mac.doFinal(lastBody.get())),
        lastHeaders.get("X-maestro-signature-256"));
  }

  @Test
  public void testNoSignatureHeaderWithoutSecret() {
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(), url, Collections.emptySet(), TIMEOUT, null, MAPPER);

    publisher.send(sampleEvent());

    Assert.assertEquals(1, requestCount.get());
    Assert.assertFalse(lastHeaders.containsKey("X-maestro-signature-256"));
  }

  @Test
  public void testEventTypeFiltering() {
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(),
            url,
            EnumSet.of(MaestroEvent.Type.STEP_INSTANCE_STATUS_CHANGE_EVENT),
            TIMEOUT,
            null,
            MAPPER);

    publisher.send(sampleEvent()); // WORKFLOW_VERSION_CHANGE_EVENT: filtered out

    Assert.assertEquals(0, requestCount.get());
  }

  @Test
  public void testClientErrorIsNotRetryable() {
    statusToReturn.set(400);
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(), url, Collections.emptySet(), TIMEOUT, null, MAPPER);

    AssertHelper.assertThrows(
        "4xx means the receiver rejected the payload",
        MaestroUnprocessableEntityException.class,
        "rejected a maestro event",
        () -> publisher.send(sampleEvent()));
  }

  @Test
  public void testServerErrorIsRetryable() {
    statusToReturn.set(503);
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(), url, Collections.emptySet(), TIMEOUT, null, MAPPER);

    AssertHelper.assertThrows(
        "5xx is retryable",
        MaestroRetryableError.class,
        "will retry",
        () -> publisher.send(sampleEvent()));
  }

  @Test
  public void testConnectionFailureIsRetryable() {
    server.stop(0);
    WebhookNotificationPublisher publisher =
        new WebhookNotificationPublisher(
            HttpClient.newHttpClient(), url, Collections.emptySet(), TIMEOUT, null, MAPPER);

    AssertHelper.assertThrows(
        "connection failures are retryable",
        MaestroRetryableError.class,
        "will retry",
        () -> publisher.send(sampleEvent()));
  }

  private MaestroEvent sampleEvent() {
    return WorkflowVersionChangeEvent.builder()
        .workflowId("sample-wf")
        .workflowName("sample-wf-name")
        .clusterName("test-cluster")
        .build();
  }
}
