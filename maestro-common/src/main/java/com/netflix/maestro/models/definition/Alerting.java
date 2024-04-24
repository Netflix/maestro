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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.alerting.AlertType;
import com.netflix.maestro.models.definition.alerting.AlertingTypeConfig;
import com.netflix.maestro.models.definition.alerting.BypassDigestConfig;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.validations.TctConstraint;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.Getter;

/** Alerting config. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "emails",
      "pagerduties",
      "slack",
      "bypass_digest_config",
      "pagerduty_configs",
      "tct",
      "type_configs"
    },
    alphabetic = true)
@Data
public class Alerting {

  private Set<String> emails;
  private Set<String> pagerduties;

  @JsonProperty("slack")
  private SlackConfig slackConfig;

  private BypassDigestConfig bypassDigestConfig;

  private Map<AlertType, AlertingTypeConfig> typeConfigs;

  private PagerdutyConfig pagerdutyConfig;

  @TctConstraint private Tct tct;

  /** Slack portion of {@link Alerting}. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "users",
        "channels",
        "mention_users",
      },
      alphabetic = true)
  @Data
  public static class SlackConfig implements Serializable {

    private static final long serialVersionUID = -5354026172019808586L;

    private Set<String> users;
    private Set<String> channels;
    private Set<String> mentionUsers;
  }

  /** Pagerduty configurations of {@link Alerting}. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "severity",
        "always_page",
      },
      alphabetic = true)
  @Data
  public static class PagerdutyConfig implements Serializable {

    private static final long serialVersionUID = -5354026172019808586L;

    private Severity severity;
    private Boolean alwaysPage;

    /**
     * <a
     * href="https://support.pagerduty.com/docs/dynamic-notifications#eventalert-severity-levels">Pagerduty
     * severity</a>.
     */
    public enum Severity {
      /** critical. */
      CRITICAL("critical"),

      /** error. */
      ERROR("error"),

      /** warning. */
      WARNING("warning"),

      /** info. */
      INFO("info");

      @JsonIgnore @Getter private final String name;

      Severity(final String name) {
        this.name = name;
      }

      /** Static creator. */
      @JsonCreator
      public static Severity create(@NotNull String name) {
        return Severity.valueOf(name.toUpperCase(Locale.US));
      }
    }
  }

  /** Update alerting fields by parsing parameters within it. */
  @JsonIgnore
  public void update(Function<ParamDefinition, Parameter> paramParser) {
    if (emails != null && !emails.isEmpty()) {
      emails =
          emails.stream()
              .map(
                  email -> {
                    ParamDefinition emailParam = ParamDefinition.buildParamDefinition(email, email);
                    Parameter param = paramParser.apply(emailParam);
                    return param == null ? email : param.asString();
                  })
              .collect(Collectors.toSet());
    }
    if (tct != null) {
      Parameter param = paramParser.apply(tct.getCompletedByTsParam());
      if (param != null) {
        tct.setCompletedByTs(param.asLong());
      }
    }
  }
}
