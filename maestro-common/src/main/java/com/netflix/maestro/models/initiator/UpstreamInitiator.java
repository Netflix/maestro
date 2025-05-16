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
package com.netflix.maestro.models.initiator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.IdHelper;
import java.util.List;
import lombok.Data;

/**
 * Upstream workflow initiator with its parent workflow and the step (subworkflow or foreach or
 * template) info.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public abstract class UpstreamInitiator implements Initiator {
  private List<Info> ancestors;

  /** setter with a validation check. */
  public void setAncestors(List<Info> ancestors) {
    Checks.checkTrue(
        ancestors != null && !ancestors.isEmpty(),
        "ancestors of upstream initiator cannot be empty or null");
    this.ancestors = ancestors;
  }

  /** helper method to get the parent. */
  @JsonIgnore
  @Override
  public Info getParent() {
    return ancestors.getLast();
  }

  /** helper method to get the root. */
  @JsonIgnore
  public Info getRoot() {
    return ancestors.getFirst();
  }

  /**
   * Get Parameter source for upstream initiator.
   *
   * @return source of the param
   */
  @JsonIgnore
  public abstract ParamSource getParameterSource();

  /** Helper method to get the closest non-inline ancestor info. */
  @JsonIgnore
  public Info getNonInlineParent() {
    return ancestors.stream()
        .reduce((i1, i2) -> i2.isInline() ? i1 : i2)
        .orElseThrow(
            () ->
                new MaestroUnprocessableEntityException(
                    "Invalid ancestors [%s] in upstream initiator as it does not contain any non inline parent.",
                    ancestors));
  }

  /** helper method to get the depth. */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Override
  public int getDepth() {
    return ancestors == null ? 0 : ancestors.size();
  }

  /** workflow step initiator information. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {"workflow_id", "instance_id", "run_id", "step_id", "step_attempt_id"},
      alphabetic = true)
  @Data
  public static class Info {
    private String workflowId;
    private long instanceId;
    private long runId;
    private String stepId;
    private long stepAttemptId;

    @Override
    public String toString() {
      return String.format(
          "[%s][%s][%s][%s][%s]", workflowId, instanceId, runId, stepId, stepAttemptId);
    }

    @JsonIgnore
    public boolean isInline() {
      return IdHelper.isInlineWorkflowId(workflowId);
    }
  }

  /** Create a concrete upstream initiator based on the given type. */
  public static UpstreamInitiator withType(Type type) {
    switch (type) {
      case SUBWORKFLOW:
        return new SubworkflowInitiator();
      case FOREACH:
        return new ForeachInitiator();
      case TEMPLATE:
        return new TemplateInitiator();
      default:
        throw new MaestroInternalError("Initiator type [%s] cannot be used as UPSTREAM", type);
    }
  }

  @JsonIgnore
  @Override
  public TimelineEvent getTimelineEvent() {
    return TimelineLogEvent.info(
        "%s step (%s) runs a new workflow instance", getType().name(), getParent());
  }
}
