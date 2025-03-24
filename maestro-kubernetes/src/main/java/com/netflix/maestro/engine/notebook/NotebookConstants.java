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

import com.netflix.maestro.models.definition.StepType;
import java.util.Locale;

/** Constants for Notebook runtime. */
final class NotebookConstants {

  private NotebookConstants() {}

  /** Notebook parameter key. */
  static final String NOTEBOOK_KEY = StepType.NOTEBOOK.getType().toLowerCase(Locale.US);

  /** Notebook input path parameter key. */
  static final String NOTEBOOK_INPUT_PARAM = "input_path";

  /** Notebook git repo parameter key. */
  static final String NOTEBOOK_GIT_REPO = "git_repo";

  /** Notebook git repo clone folder name. */
  static final String NOTEBOOK_GIT_CLONE_FOLDER = "maestro_notebook_git_repo";

  /** Notebook git branch parameter key. */
  static final String NOTEBOOK_GIT_BRANCH = "git_branch";

  /** Notebook git commit parameter key. */
  static final String NOTEBOOK_GIT_COMMIT = "git_commit";

  /** Notebook git submodules flag parameter key. */
  static final String NOTEBOOK_GIT_SUBMODULES = "git_submodules";

  /** Notebook output path parameter key. */
  static final String NOTEBOOK_OUTPUT_PARAM = "output_path";

  /** Papermill arguments parameter path. */
  static final String PAPERMILL_ARG_PARAM = "papermill_arguments";

  /** Papermill command. */
  static final String PAPERMILL_COMMAND = "papermill";

  /** Notebook output prefix. */
  static final String NOTEBOOK_INIT_COMMAND = "init_command";

  /** Log output argument. */
  static final String LOG_OUTPUT_ARG = "--log-output";

  /** Start timeout argument. */
  static final String START_TIMEOUT_ARG = "--start_timeout";

  /** Start timeout default. */
  static final long START_TIMEOUT_DEFAULT = 600;

  /** Delimiter for command line. */
  static final String PARAM_DELIMITER = " ";

  /** Notebook criticality key. */
  static final String CRITICALITY = "criticality";

  /** Notebook attempt number (for now, it represents attempts in last run). */
  static final String ATTEMPT_NUMBER = "attempt_number";

  /** Notebook pager duties. */
  static final String PAGER_DUTIES = "pager_duties";

  /** Notebook notification emails. */
  static final String NOTIFICATION_EMAILS = "notification_emails";

  /** TCT (Target Completion Time) parameter. */
  static final String TCT_PARAM = "tct";

  /** TCT completed by hour parameter. */
  static final String TCT_COMPLETED_BY_HOUR = "completed_by_hour";

  /** TCT duration minutes parameter. */
  static final String TCT_DURATION_MINUTES = "duration_minutes";

  /** TCT completed by timestamp parameter. */
  static final String TCT_COMPLETED_BY_TS = "completed_by_ts";

  /** TCT time zone parameter. */
  static final String TCT_TZ = "tz";

  /** Slack parameter. */
  static final String SLACK_PARAM = "slack";

  /** Slack users parameter. */
  static final String SLACK_USERS = "users";

  /** Slack channels parameter. */
  static final String SLACK_CHANNELS = "channels";

  /** Step timeout parameter. */
  static final String STEP_TIMEOUT = "StepTimeout";
}
