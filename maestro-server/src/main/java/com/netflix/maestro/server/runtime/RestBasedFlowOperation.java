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
package com.netflix.maestro.server.runtime;

import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.engine.FlowExecutor;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.properties.FlowEngineProperties;
import com.netflix.maestro.flow.runtime.FlowOperation;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.springframework.web.client.RestTemplate;

public class RestBasedFlowOperation implements FlowOperation {

  private final RestTemplate restTemplate;
  private final MaestroFlowDao flowDao;
  private final FlowExecutor flowExecutor;
  private final String localAddress;
  private final long expirationDuration;
  private final Map<Long, FlowGroup> addressCache;

  public RestBasedFlowOperation(
      RestTemplate restTemplate,
      MaestroFlowDao flowDao,
      FlowExecutor executor,
      FlowEngineProperties properties) {
    this.restTemplate = restTemplate;
    this.flowDao = flowDao;
    this.flowExecutor = executor;
    this.localAddress = properties.getEngineAddress();
    this.expirationDuration = TimeUnit.SECONDS.toMillis(properties.getExpirationInSecs());
    this.addressCache = new ConcurrentHashMap<>();
  }

  @Override
  public String startFlow(
      long groupId,
      String flowId,
      String flowReference,
      FlowDef flowDef,
      Map<String, Object> flowInput) {
    try {
      FlowGroup group = loadFlowGroup(groupId);
      if (group == null || localAddress.equals(group.address())) {
        return flowExecutor.startFlow(groupId, flowId, flowReference, flowDef, flowInput);
      } else {
        return restTemplate.postForObject(
            group.address() + "/api/v3/groups/{groupId}/flows/{flowReference}/start",
            Map.of(
                "flowId", flowId,
                "flowDef", flowDef,
                "flowInput", flowInput),
            String.class,
            groupId,
            flowReference);
      }
    } catch (MaestroRetryableError e) {
      addressCache.remove(groupId);
      throw e;
    }
  }

  @Override
  public boolean wakeUp(long groupId, String flowReference, String taskReference) {
    try {
      FlowGroup group = loadFlowGroup(groupId);
      if (group == null || localAddress.equals(group.address())) {
        return flowExecutor.wakeUp(groupId, flowReference, taskReference);
      } else {
        return Boolean.TRUE.equals(
            restTemplate.postForObject(
                group.address()
                    + "/api/v3/groups/{groupId}/flows/{flowReference}/tasks/{taskReference}/notify",
                null,
                Boolean.class,
                groupId,
                flowReference,
                taskReference));
      }
    } catch (MaestroRetryableError e) {
      addressCache.remove(groupId);
      throw e;
    }
  }

  @Override
  public boolean wakeUp(long groupId, Set<String> refs) {
    try {
      FlowGroup group = loadFlowGroup(groupId);
      if (group == null || localAddress.equals(group.address())) {
        return refs.stream().allMatch(ref -> flowExecutor.wakeUp(groupId, ref, null));
      } else {
        return Boolean.TRUE.equals(
            restTemplate.postForObject(
                group.address() + "/api/v3/groups/{groupId}/flows/notify",
                refs,
                Boolean.class,
                groupId));
      }
    } catch (MaestroRetryableError e) {
      addressCache.remove(groupId);
      throw e;
    }
  }

  private FlowGroup loadFlowGroup(long groupId) {
    FlowGroup group = addressCache.get(groupId);
    if (group != null && group.heartbeatTs() + expirationDuration > System.currentTimeMillis()) {
      return group;
    }
    group = flowDao.getGroup(groupId);
    if (group == null) {
      return group;
    }
    if (group.heartbeatTs() + expirationDuration > System.currentTimeMillis()) {
      addressCache.put(groupId, group);
      return group;
    }
    throw new MaestroRetryableError("Group [%s] is expired and will retry", group);
  }
}
