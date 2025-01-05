package com.netflix.maestro.flow.models;

import java.util.List;
import lombok.Data;

/**
 * Flow definition data model. Note that flow has timeout trigger but timeout is handled by the
 * Maestro engine.
 *
 * @author jun-he
 */
@Data
public class FlowDef {
  private TaskDef prepareTask; // TaskDef for the task to run at the beginning before user tasks
  private TaskDef monitorTask; // TaskDef for the task to run whenever there is an update
  private List<List<TaskDef>> tasks; // tasks in the flow graph

  private long timeoutInMillis;
  private boolean finalFlowStatusCallbackEnabled;
}
