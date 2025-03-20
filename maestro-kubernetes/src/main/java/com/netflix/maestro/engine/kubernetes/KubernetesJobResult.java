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
package com.netflix.maestro.engine.kubernetes;

import com.netflix.maestro.engine.steps.StepRuntime;

/**
 * Kubernetes job result. Won't be persisted.
 *
 * @param jobId Job ID
 * @param jobStatus Job status
 */
public record KubernetesJobResult(String jobId, StepRuntime.State jobStatus) {}
