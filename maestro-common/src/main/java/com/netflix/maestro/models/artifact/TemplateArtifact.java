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
package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import lombok.Data;

/**
 * Template artifact to store the inline workflow instances started by a template step at runtime. A
 * step preparer runs its generator in a prepare instance before the execution instance is launched;
 * both are instances of the same inline workflow id and share the run id. It also records the job
 * type and the template version resolved at start, so a run in flight keeps its version if the
 * template is republished, and the expansion the preparer returned.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "template_workflow_id",
      "template_identity",
      "prepare_type",
      "prepare_instance_id",
      "template_instance_id",
      "template_run_id",
      "template_uuid",
      "job_type",
      "template_version",
      "expansion",
      "template_overview"
    },
    alphabetic = true)
@Data
public final class TemplateArtifact implements Artifact {
  private String templateWorkflowId; // template inline workflow id
  private String templateIdentity; // template inline workflow identity
  private String prepareType; // preparer type of the registered template
  private Long prepareInstanceId; // inline instance id of the prepare instance, step preparer only
  private Long templateInstanceId; // inline instance id of the execution instance, null until ready
  private long templateRunId; // inline run id, shared by the prepare and execution instances
  private String templateUuid; // inline instance uuid of the execution instance
  private String jobType; // template step sub_type
  private String templateVersion; // job template version resolved at start
  private TemplateExpansion expansion; // what the preparer returned, before step inputs are added
  private WorkflowRuntimeOverview templateOverview;

  @JsonIgnore
  @Override
  public TemplateArtifact asTemplate() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.TEMPLATE;
  }

  @JsonIgnore
  public String getIdentity() {
    return String.format(
        "[%s][%s][%s][%s]", templateWorkflowId, templateInstanceId, templateRunId, templateUuid);
  }
}
