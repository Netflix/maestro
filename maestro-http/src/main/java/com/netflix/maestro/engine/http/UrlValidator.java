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
package com.netflix.maestro.engine.http;

import com.netflix.maestro.engine.properties.HttpStepProperties;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.utils.ObjectHelper;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * URL validator to prevent Server-Side Request Forgery (SSRF) attacks by validating URLs before
 * making HTTP requests.
 *
 * <p>This validator uses an allow-list approach for maximum security:
 *
 * <ul>
 *   <li>Only allows HTTP/HTTPS protocols
 *   <li>Only allows hostnames in the configured allow-list
 * </ul>
 */
@Slf4j
public class UrlValidator {
  private static final Set<String> ALLOWED_SCHEMES = Set.of("HTTP", "HTTPS");

  private final Set<String> allowList;

  /**
   * Constructor.
   *
   * @param properties http step properties
   */
  public UrlValidator(HttpStepProperties properties) {
    allowList =
        ObjectHelper.valueOrDefault(properties.getAllowList(), Set.<String>of()).stream()
            .map(s -> s.toLowerCase(Locale.US))
            .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Validate and parse a URL string to prevent SSRF attacks.
   *
   * @param url the URL string to validate and parse
   * @return the parsed URI
   * @throws MaestroValidationException if the URL is invalid or not allowed
   */
  public URI validateAndParseUri(String url) {
    try {
      URI uri = URI.create(url);
      String scheme = uri.getScheme();
      if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toUpperCase(Locale.US))) {
        LOG.info(
            "Rejected URL with invalid scheme. URL: [{}], Scheme: [{}], Allowed schemes: {}",
            url,
            scheme,
            ALLOWED_SCHEMES);
        throw new MaestroValidationException(
            "URL scheme [%s] is not allowed. Only %s are allowed.", scheme, ALLOWED_SCHEMES);
      }
      String host = uri.getHost();
      String normalizedHost = host != null ? host.toLowerCase(Locale.US) : null;
      if (normalizedHost == null || !allowList.contains(normalizedHost)) {
        LOG.info(
            "Rejected URL with disallowed host. URL: [{}], Host: [{}], Allowed hosts: {}",
            url,
            normalizedHost,
            allowList);
        throw new MaestroValidationException(
            "URL host [%s] is not allowed. Please contact the administrator.", host);
      }
      return uri;
    } catch (NullPointerException | IllegalArgumentException e) {
      LOG.info("Rejected malformed URL: [{}]", url, e);
      throw new MaestroValidationException(e, "Invalid URL: [%s]", url);
    }
  }
}
