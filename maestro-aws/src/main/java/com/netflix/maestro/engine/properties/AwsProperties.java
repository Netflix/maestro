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
package com.netflix.maestro.engine.properties;

import com.netflix.maestro.annotations.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

/** AWS configuration properties. */
@AllArgsConstructor
@Getter
@ConstructorBinding
@ConfigurationProperties("aws")
public class AwsProperties {
  private final SqsProperties sqs;
  private final SnsProperties sns;

  public record SnsProperties(String topic, @Nullable String endpoint) {}
}
