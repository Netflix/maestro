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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.properties.HttpStepProperties;
import com.netflix.maestro.exceptions.MaestroValidationException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class UrlValidatorTest {

  private UrlValidator validator;

  @Before
  public void setUp() {
    HttpStepProperties properties = new HttpStepProperties();
    properties.setAllowList(Set.of("example.com", "api.EXAMPLE.com"));
    validator = new UrlValidator(properties);
  }

  @Test
  public void testValidUrl() {
    for (String url :
        Arrays.asList(
            "http://api.example.com",
            "http://example.com:8080/path",
            "https://example.com",
            "https://example.com/path",
            "https://api.example.com/v1/resource",
            "https://api.example.com:443/secure",
            "https://EXAMPLE.COM",
            "https://Example.Com",
            "https://API.EXAMPLE.COM",
            "http://user:pass@example.com/path",
            "http://example.com/path%00with%20encoded",
            "http://example.com?param=value%00",
            "http://example.com:80",
            "http://example.com:8080",
            "https://example.com:443",
            "https://example.com:9999")) {
      assertEquals(URI.create(url), validator.validateAndParseUri(url));
    }
  }

  @Test
  public void testInvalidUrl() {
    // Test disallowed hosts
    for (String url :
        Arrays.asList(
            "http://not.allowed.com/admin",
            "http://other.example.com",
            "http://localhost:8080",
            "http://user:pass@disallowed.com/path",
            "http://subdomain.example.com",
            "http://example.com.evil.com",
            "http://example%2Ecom.evil.com",
            "http://evil.com:80")) {
      AssertHelper.assertThrows(
          "Disallowed host",
          MaestroValidationException.class,
          "is not allowed. Please contact the administrator.",
          () -> validator.validateAndParseUri(url));
    }

    // Test disallowed schemes
    for (String url :
        Arrays.asList(
            "file:///etc/passwd",
            "ftp://example.com/resource",
            "jar:file:/path/to/jar!/resource",
            "example.com")) {
      AssertHelper.assertThrows(
          "Disallowed scheme",
          MaestroValidationException.class,
          "URL scheme",
          () -> validator.validateAndParseUri(url));
    }

    // Test malformed URLs
    for (String url : Arrays.asList(null, " ", "http://", "invalid url")) {
      AssertHelper.assertThrows(
          "Malformed URL",
          MaestroValidationException.class,
          "BAD_REQUEST - Invalid URL: [" + url + "]",
          () -> validator.validateAndParseUri(url));
    }

    AssertHelper.assertThrows(
        "Null URL",
        MaestroValidationException.class,
        "URL scheme [null] is not allowed.",
        () -> validator.validateAndParseUri(""));
  }

  @Test
  public void testIpAddresses() {
    for (String url :
        Arrays.asList(
            "http://127.0.0.1:8080/admin",
            "http://10.0.0.1/admin",
            "http://172.16.0.1/admin",
            "http://192.168.1.1/admin",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:8080/admin",
            "http://[2001:db1::1]/admin")) {
      AssertHelper.assertThrows(
          "IP address not allowed",
          MaestroValidationException.class,
          "is not allowed.",
          () -> validator.validateAndParseUri(url));
    }
  }

  @Test
  public void testEmptyOrNullAllowList() {
    HttpStepProperties properties = new HttpStepProperties();
    properties.setAllowList(Set.of());
    UrlValidator emptyValidator = new UrlValidator(properties);

    AssertHelper.assertThrows(
        "Empty allowlist blocks all",
        MaestroValidationException.class,
        "is not allowed.",
        () -> emptyValidator.validateAndParseUri("http://example.com"));

    properties = new HttpStepProperties();
    properties.setAllowList(null);
    UrlValidator nullValidator = new UrlValidator(properties);
    AssertHelper.assertThrows(
        "Null allowlist blocks all",
        MaestroValidationException.class,
        "is not allowed.",
        () -> nullValidator.validateAndParseUri("https://example.com"));
  }
}
