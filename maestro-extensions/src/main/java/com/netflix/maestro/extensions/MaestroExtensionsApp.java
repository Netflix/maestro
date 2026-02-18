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
package com.netflix.maestro.extensions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/** Maestro Extensions Spring Boot app for foreach flattening service. */
@Slf4j
@SpringBootApplication(
    exclude = {
      FlywayAutoConfiguration.class,
      DataSourceAutoConfiguration.class,
      JacksonAutoConfiguration.class,
    })
public class MaestroExtensionsApp {

  /** Constructor. */
  protected MaestroExtensionsApp() {}

  /**
   * Spring app main class.
   *
   * @param args input arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(MaestroExtensionsApp.class, args);
    LOG.info("========== Maestro Extensions app started. ==========");
  }
}
