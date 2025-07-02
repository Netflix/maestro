/*
 * Copyright (c) 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.maestro.engine.properties;

import lombok.Getter;
import lombok.Setter;

/** Properties for Redis. */
@Getter
@Setter
public class RedisProperties {
  private static final RedisServerType REDIS_SERVER_TYPE_DEFAULT_VALUE = RedisServerType.SINGLE;
  private static final String REDIS_SERVER_STRING_DEFAULT_VALUE = "redis://127.0.0.1:6379";
  private static final int DEFAULT_REDIS_CONNECTION_TIMEOUT = 10000;
  private static final int DEFAULT_REDIS_SCAN_INTERVAL = 2000;

  /** get redis server type. */
  private RedisServerType redisServerType = REDIS_SERVER_TYPE_DEFAULT_VALUE;

  /** get redis server address. */
  private String redisServerAddress = REDIS_SERVER_STRING_DEFAULT_VALUE;

  /** get redis connection timeout. */
  private int redisConnectionTimeout = DEFAULT_REDIS_CONNECTION_TIMEOUT;

  /** get redis scan interval. */
  private int redisScanInterval = DEFAULT_REDIS_SCAN_INTERVAL;

  public enum RedisServerType {
    /** SINGLE: single redis instance. */
    SINGLE,
    /** CLUSTER: redis cluster. */
    CLUSTER,
    /** SENTINEL: redis sentinel. */
    SENTINEL
  }
}
