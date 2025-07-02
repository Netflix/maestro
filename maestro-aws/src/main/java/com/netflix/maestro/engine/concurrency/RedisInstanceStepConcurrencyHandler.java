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
package com.netflix.maestro.engine.concurrency;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.utils.ObjectHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScript;

/** Redis based instance_step_concurrency handler implementation. */
@Slf4j
@AllArgsConstructor
public class RedisInstanceStepConcurrencyHandler implements InstanceStepConcurrencyHandler {
  private static final String ADD_KEY_INTO_SET_STMT =
      "return redis.call('SADD', KEYS[1], ARGV[1]);";
  private static final String REMOVE_KEY_FROM_SET_STMT =
      "return redis.call('SREM', KEYS[1], ARGV[1]);";
  private static final String ADD_INSTANCE_INTO_SET_STMT =
      "if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 1; end; "
          + "local limit = tonumber(ARGV[2]); "
          + "if redis.call('SCARD', KEYS[2]) >= limit then return 0; end; "
          + "if redis.call('SCARD', KEYS[1]) >= limit then return 0; end; "
          + "redis.call('SADD', KEYS[1], ARGV[1]); return 1;";
  private static final String CLEAN_UP_ALL_DATA_STMT = "return redis.call('DEL', unpack(KEYS));";

  private final RScript redisLua;
  private final MaestroMetrics metrics;

  /** Add a step uuid into the uuid set for a given concurrencyId. */
  @Override
  public Optional<Details> registerStep(String concurrencyId, String uuid) {
    try {
      String idWithHash = withHashTag(concurrencyId, 0);
      Long res =
          redisLua.eval(
              RScript.Mode.READ_WRITE,
              ADD_KEY_INTO_SET_STMT,
              RScript.ReturnType.INTEGER,
              Collections.singletonList(idWithHash),
              uuid);
      LOG.debug("Register a step: [{}][{}] with a result: [{}]", idWithHash, uuid, res);
      return Optional.empty();
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
          getClass(),
          AwsMetricConstants.TYPE_TAG,
          "failedRegisterStep");
      return Optional.of(Details.create(e, true, "Failed to register a step: " + uuid));
    }
  }

  /**
   * Unregister a step uuid from the uuid set for a given concurrencyId. If failed, it throws a
   * MaestroRetryableError.
   */
  @Override
  public void unregisterStep(String concurrencyId, String uuid) {
    try {
      String idWithHash = withHashTag(concurrencyId, 0);
      Long res =
          redisLua.eval(
              RScript.Mode.READ_WRITE,
              REMOVE_KEY_FROM_SET_STMT,
              RScript.ReturnType.INTEGER,
              Collections.singletonList(idWithHash),
              uuid);
      LOG.debug("Unregister a step: [{}][{}] with a result: [{}]", idWithHash, uuid, res);
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
          getClass(),
          AwsMetricConstants.TYPE_TAG,
          "failedUnregisterStep");
      throw new MaestroRetryableError(
          e, "Failed to unregister a step [%s] for concurrencyId [%s]", uuid, concurrencyId);
    }
  }

  private String withHashTag(String concurrencyId, int depth) {
    return "{" + concurrencyId + "}:" + depth;
  }

  /**
   * Add an instance uuid into the uuid set for a given concurrencyId with depth. It will check if
   * either instance uuid set or step uuid set reach the limit. If yes, return false; otherwise
   * return true. If there is any exception, also return false. When instance_step_concurrency is
   * unset meaning disabled, it will only check the instance uuid set but the step uuid set will not
   * be used for throttling (no step is registered so just an always-true check).
   */
  @Override
  public boolean addInstance(RunRequest runRequest) {
    long limit =
        ObjectHelper.valueOrDefault(
            runRequest.getInstanceStepConcurrency(), Defaults.DEFAULT_INSTANCE_STEP_CONCURRENCY);
    int depth = runRequest.getInitiator().getDepth();
    String uuid = runRequest.getRequestId().toString();

    String idWithHash = withHashTag(runRequest.getCorrelationId(), 0);
    String idWithDepth = withHashTag(runRequest.getCorrelationId(), depth);
    try {
      Boolean res =
          redisLua.eval(
              RScript.Mode.READ_WRITE,
              ADD_INSTANCE_INTO_SET_STMT,
              RScript.ReturnType.BOOLEAN,
              Arrays.asList(idWithDepth, idWithHash),
              uuid,
              String.valueOf(limit));
      LOG.debug(
          "Add an instance: [{}][{}] for limit [{}] with a result: [{}]",
          idWithDepth,
          uuid,
          limit,
          res);
      return res;
    } catch (Exception e) {
      LOG.warn(
          "Cannot add instance for concurrency id [{}], depth [{}], uuid [{}] due to ",
          idWithHash,
          depth,
          uuid,
          e);
      metrics.counter(
          AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
          getClass(),
          AwsMetricConstants.TYPE_TAG,
          "failedAddInstance");
      return false;
    }
  }

  /**
   * Remove an instance uuid from the uuid set for a given concurrencyId with depth. If failed, it
   * throws a MaestroRetryableError.
   */
  @Override
  public void removeInstance(String concurrencyId, int depth, String uuid) {
    String idWithDepth = withHashTag(concurrencyId, depth);
    try {
      Long res =
          redisLua.eval(
              RScript.Mode.READ_WRITE,
              REMOVE_KEY_FROM_SET_STMT,
              RScript.ReturnType.INTEGER,
              Collections.singletonList(idWithDepth),
              uuid);
      LOG.debug("Remove an instance: [{}][{}] with a result: [{}]", idWithDepth, uuid, res);
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
          getClass(),
          AwsMetricConstants.TYPE_TAG,
          "failedRemoveInstance");
      throw new MaestroRetryableError(
          e,
          "Failed to remove instance for concurrency id [%s], depth [%s], uuid [%s]",
          concurrencyId,
          depth,
          uuid);
    }
  }

  @Override
  public void cleanUp(String concurrencyId) {
    List<Object> ids =
        IntStream.range(0, Constants.WORKFLOW_DEPTH_LIMIT)
            .mapToObj(i -> withHashTag(concurrencyId, i))
            .collect(Collectors.toList());
    try {
      Long res =
          redisLua.eval(
              RScript.Mode.READ_WRITE, CLEAN_UP_ALL_DATA_STMT, RScript.ReturnType.INTEGER, ids);
      LOG.debug("Cleanup all sets: {} with a result: [{}]", ids, res);
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.INSTANCE_STEP_CONCURRENCY_ERROR_METRIC,
          getClass(),
          AwsMetricConstants.TYPE_TAG,
          "failedCleanUp");
      throw new MaestroRetryableError(e, "Failed to cleanup all sets for [%s]", concurrencyId);
    }
  }
}
