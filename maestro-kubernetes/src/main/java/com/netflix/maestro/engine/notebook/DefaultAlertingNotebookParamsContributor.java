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
package com.netflix.maestro.engine.notebook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.models.definition.DefaultAlerting;
import com.netflix.maestro.models.definition.Tct;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link NotebookParamsContributor} that surfaces the {@link DefaultAlerting} shape (emails,
 * pagerduties, slack, tct) into notebook params. Noops if the workflow's alerting block is null or
 * a non-default {@link Alerting} implementation.
 */
@Slf4j
@AllArgsConstructor
public class DefaultAlertingNotebookParamsContributor implements NotebookParamsContributor {
  private final ObjectMapper objectMapper;

  @Override
  public void contribute(WorkflowSummary summary, Map<String, Object> paramMap) {
    Alerting alerting = summary.getRunProperties().getAlerting();
    if (!(alerting instanceof DefaultAlerting defaultAlerting)) {
      return;
    }
    addTctParamsIfPresent(paramMap, defaultAlerting);
    Set<String> pagerDuties = defaultAlerting.getPagerduties();
    Set<String> emails = defaultAlerting.getEmails();
    if (pagerDuties != null && !pagerDuties.isEmpty()) {
      paramMap.put(NotebookConstants.PAGER_DUTIES, pagerDuties);
    }
    if (emails != null && !emails.isEmpty()) {
      paramMap.put(NotebookConstants.NOTIFICATION_EMAILS, emails);
    }
    DefaultAlerting.SlackConfig slackConfig = defaultAlerting.getSlackConfig();
    if (slackConfig != null) {
      addSlackParamsIfPresent(summary, paramMap, slackConfig);
    }
  }

  private void addSlackParamsIfPresent(
      WorkflowSummary summary,
      Map<String, Object> paramMap,
      DefaultAlerting.SlackConfig slackConfig) {
    Map<String, String> slackParams = new LinkedHashMap<>();
    Set<String> users = slackConfig.getUsers();
    try {
      if (users != null && !users.isEmpty()) {
        slackParams.put(NotebookConstants.SLACK_USERS, objectMapper.writeValueAsString(users));
      }
      Set<String> channels = slackConfig.getChannels();
      if (channels != null && !channels.isEmpty()) {
        slackParams.put(
            NotebookConstants.SLACK_CHANNELS, objectMapper.writeValueAsString(channels));
      }
    } catch (JsonProcessingException e) {
      String workflowIdentity = summary.getIdentity();
      LOG.warn("Failed to serialize slack params for workflow {}", workflowIdentity, e);
      throw new MaestroBadRequestException(
          e, "Failed to serialize slack params for workflow %s", workflowIdentity);
    }
    if (!slackParams.isEmpty()) {
      paramMap.put(NotebookConstants.SLACK_PARAM, slackParams);
    }
  }

  private void addTctParamsIfPresent(Map<String, Object> paramMap, DefaultAlerting alerting) {
    Tct tct = alerting.getTct();
    if (tct == null) {
      return;
    }
    Map<String, String> tctMap = new LinkedHashMap<>();
    if (tct.getCompletedByHour() != null) {
      tctMap.put(NotebookConstants.TCT_COMPLETED_BY_HOUR, tct.getCompletedByHour().toString());
    }
    if (tct.getDurationMinutes() != null) {
      tctMap.put(NotebookConstants.TCT_DURATION_MINUTES, tct.getDurationMinutes().toString());
    }
    if (tct.getCompletedByTs() != null) {
      tctMap.put(NotebookConstants.TCT_COMPLETED_BY_TS, tct.getCompletedByTs().toString());
    }
    if (tct.getTz() != null) {
      tctMap.put(NotebookConstants.TCT_TZ, tct.getTz());
    }
    if (!tctMap.isEmpty()) {
      paramMap.put(NotebookConstants.TCT_PARAM, tctMap);
    }
  }
}
