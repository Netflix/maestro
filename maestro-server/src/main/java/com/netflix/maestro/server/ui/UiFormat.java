/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.server.ui;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.springframework.stereotype.Component;

/**
 * Small formatting helper exposed to the read-only UI Thymeleaf templates as {@code @uiFmt}. It
 * renders epoch-millis timestamps, durations, and nested objects as pretty JSON so the templates
 * stay free of inline logic.
 */
@Component("uiFmt")
public class UiFormat {
  private static final DateTimeFormatter TS_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneOffset.UTC);

  private static final String EMPTY = "-";

  private final ObjectWriter prettyWriter;

  /** Constructor. */
  public UiFormat(ObjectMapper objectMapper) {
    this.prettyWriter = objectMapper.writerWithDefaultPrettyPrinter();
  }

  /** Format an epoch-millis timestamp as a UTC string, or {@code "-"} when null. */
  public String time(Long epochMillis) {
    if (epochMillis == null || epochMillis <= 0) {
      return EMPTY;
    }
    return TS_FORMAT.format(Instant.ofEpochMilli(epochMillis));
  }

  /** Human-readable duration between two epoch-millis timestamps, or {@code "-"} when unknown. */
  public String duration(Long startMillis, Long endMillis) {
    if (startMillis == null || startMillis <= 0 || endMillis == null || endMillis <= 0) {
      return EMPTY;
    }
    Duration d = Duration.ofMillis(Math.max(0, endMillis - startMillis));
    long h = d.toHours();
    long m = d.toMinutesPart();
    long s = d.toSecondsPart();
    StringBuilder sb = new StringBuilder();
    if (h > 0) {
      sb.append(h).append("h ");
    }
    if (h > 0 || m > 0) {
      sb.append(m).append("m ");
    }
    return sb.append(s).append('s').toString();
  }

  /** Render any object as pretty-printed JSON for display, or {@code "-"} when null. */
  public String json(Object value) {
    if (value == null) {
      return EMPTY;
    }
    try {
      return prettyWriter.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return String.valueOf(value);
    }
  }
}
