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
package com.netflix.maestro.models.stepruntime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Titus batch options. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "application_name",
      "attributes",
      "container_attributes",
      "cpu",
      "disk_mb",
      "entrypoint",
      "environment_variables",
      "gpu",
      "iam_role",
      "image",
      "image_tag",
      "image_digest",
      "memory_mb",
      "network_mbps",
      "pending_ttl_sec",
      "platform_errors",
      "priority_value",
      "priority_class",
      "runtime_limit_sec",
      "shm_size_mb",
      "security_groups",
      "tenant_id",
      "job_group_detail",
      "job_group_sequence",
      "job_group_stack",
      "acting_on_behalf_of",
      "trigger_on_behalf_of",
      "request_metatron_identity"
    },
    alphabetic = true)
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuppressFBWarnings({"EI", "EI2"})
public class TitusCommand {
  private String applicationName;
  private Map<String, String> attributes;
  private Map<String, String> containerAttributes;
  private Double cpu;
  private Integer diskMb;
  private String entrypoint;
  private Map<String, String> environmentVariables;
  private Integer gpu;
  private String[] hardConstraints;
  private String iamRole;
  private String image;
  private String imageTag;
  private String imageDigest;
  private Integer memoryMb;
  private Integer networkMbps;
  private Integer pendingTtlSec;
  private String[] platformErrors;
  private Integer priorityValue;
  private Integer priorityClass;
  private Integer runtimeLimitSec;
  private Integer shmSizeMB;
  private String[] securityGroups;
  private String tenantId;
  private String jobGroupDetail;
  private String jobGroupSequence;
  private String jobGroupStack;
  private Map<String, String> actingOnBehalfOf;
  private Map<String, String> triggeringOnBehalfOf;
  private boolean requestMetatronIdentity;
  private String jobDeduplicationKey;
  private String networkMode;
  private String ownerEmail;
  private boolean candidateApplied;
}
