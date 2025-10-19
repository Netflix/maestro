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
package com.netflix.maestro.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.sql.DataSource;

/** Utility helper methods for unit tests and integration tests. */
public final class MaestroTestHelper {

  private MaestroTestHelper() {}

  private static final String DELETE_MAESTRO_WORKFLOW_QUERY =
      "WITH deleted_wf AS ("
          + "DELETE FROM maestro_workflow WHERE workflow_id=? RETURNING *)"
          + "INSERT INTO maestro_workflow_deleted (workflow, timeline, stage) "
          + "SELECT row_to_json(deleted_wf), ARRAY['the workflow is deleted by unit test'], 'DELETION_DONE' FROM deleted_wf";

  // deletion function for unit tests and integration tests
  public static int removeWorkflow(DataSource dataSource, String workflowId) {
    return deleteWorkflowInternal(dataSource, workflowId, true, false);
  }

  public static int deleteWorkflow(DataSource dataSource, String workflowId) {
    return deleteWorkflowInternal(dataSource, workflowId, false, true);
  }

  private static int deleteWorkflowInternal(
      DataSource dataSource,
      String workflowId,
      boolean includesWorkflowDelete,
      boolean includesWorkflowInstances) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement removeWorkflow = conn.prepareStatement(DELETE_MAESTRO_WORKFLOW_QUERY);
        PreparedStatement removeWorkflowVersion =
            conn.prepareStatement("DELETE FROM maestro_workflow_version WHERE workflow_id = ?");
        PreparedStatement removeWorkflowProps =
            conn.prepareStatement("DELETE FROM maestro_workflow_properties WHERE workflow_id = ?");
        PreparedStatement removeWorkflowTimeline =
            conn.prepareStatement("DELETE FROM maestro_workflow_timeline WHERE workflow_id = ?");
        PreparedStatement removeWorkflowDeleted =
            includesWorkflowDelete
                ? conn.prepareStatement(
                    "DELETE FROM maestro_workflow_deleted WHERE workflow_id = ?")
                : conn.prepareStatement("");
        PreparedStatement removeWorkflowInstances =
            includesWorkflowInstances
                ? conn.prepareStatement("DELETE FROM maestro_workflow_instance WHERE workflow_id=?")
                : conn.prepareStatement("");
        PreparedStatement removeStepInstances =
            includesWorkflowInstances
                ? conn.prepareStatement("DELETE FROM maestro_step_instance WHERE workflow_id=?")
                : conn.prepareStatement("")) {
      removeWorkflow.setString(1, workflowId);
      int removedCount = removeWorkflow.executeUpdate();

      removeWorkflowVersion.setString(1, workflowId);
      removedCount += removeWorkflowVersion.executeUpdate();
      removeWorkflowProps.setString(1, workflowId);
      removedCount += removeWorkflowProps.executeUpdate();
      removeWorkflowTimeline.setString(1, workflowId);
      removedCount += removeWorkflowTimeline.executeUpdate();
      if (includesWorkflowDelete) {
        removeWorkflowDeleted.setString(1, workflowId);
        removedCount += removeWorkflowDeleted.executeUpdate();
      }
      if (includesWorkflowInstances) {
        removeWorkflowInstances.setString(1, workflowId);
        removedCount += removeWorkflowInstances.executeUpdate();
        removeStepInstances.setString(1, workflowId);
        removedCount += removeStepInstances.executeUpdate();
      }
      conn.commit();
      return removedCount;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // deletion function for unit tests and integration tests
  public static int removeWorkflowInstance(
      DataSource dataSource, String workflowId, long workflowInstanceId) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement removeWorkflowInstances =
            conn.prepareStatement(
                "DELETE FROM maestro_workflow_instance WHERE workflow_id=? AND instance_id=?");
        PreparedStatement removeStepInstances =
            conn.prepareStatement(
                "DELETE FROM maestro_step_instance WHERE workflow_id=? AND workflow_instance_id=?");
        PreparedStatement updateWorkflowInstanceOverview =
            conn.prepareStatement(
                "UPDATE maestro_workflow set (latest_instance_id, modify_ts)=(latest_instance_id-1,CURRENT_TIMESTAMP) "
                    + "WHERE workflow_id=? and latest_instance_id=?")) {
      removeWorkflowInstances.setString(1, workflowId);
      removeWorkflowInstances.setLong(2, workflowInstanceId);
      int removedCount = removeWorkflowInstances.executeUpdate();
      removeStepInstances.setString(1, workflowId);
      removeStepInstances.setLong(2, workflowInstanceId);
      removedCount += removeStepInstances.executeUpdate();
      updateWorkflowInstanceOverview.setString(1, workflowId);
      updateWorkflowInstanceOverview.setLong(2, workflowInstanceId);
      removedCount += updateWorkflowInstanceOverview.executeUpdate();
      conn.commit();
      return removedCount;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // deletion function for job templates in unit tests and integration tests
  public static int removeJobTemplate(DataSource dataSource, String jobType, String tag) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement removeJobTemplate =
            conn.prepareStatement(
                tag != null
                    ? "DELETE FROM maestro_job_template WHERE job_type=? AND tag=?"
                    : "DELETE FROM maestro_job_template WHERE job_type=?")) {
      removeJobTemplate.setString(1, jobType);
      if (tag != null) {
        removeJobTemplate.setString(2, tag);
      }
      int removedCount = removeJobTemplate.executeUpdate();
      conn.commit();
      return removedCount;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
