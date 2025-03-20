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

/** Interface for Kubernetes runtime executor. */
public interface KubernetesRuntimeExecutor {
  /**
   * Launch a kubernetes job.
   *
   * @param context Kubernetes step context.
   * @return Kubernetes job result.
   */
  KubernetesJobResult launchJob(KubernetesStepContext context);

  /**
   * Check the status of a kubernetes job.
   *
   * @param jobId Job ID.
   * @return Kubernetes job result.
   */
  KubernetesJobResult checkJobStatus(String jobId);

  /**
   * Get the log of a finished kubernetes job.
   *
   * @param jobId Job ID.
   * @return Job log.
   */
  String getJobLog(String jobId);

  /**
   * Terminate a kubernetes job.
   *
   * @param jobId Job ID to terminate.
   */
  void terminateJob(String jobId);
}
