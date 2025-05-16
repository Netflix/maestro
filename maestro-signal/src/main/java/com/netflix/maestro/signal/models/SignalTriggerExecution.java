package com.netflix.maestro.signal.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Map;
import lombok.Data;

/**
 * Data model for signal trigger to execute a workflow, will be passed over the queue.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"workflow_id", "trigger_uuid", "signal_ids", "condition", "dedup_expr", "params"},
    alphabetic = true)
@Data
public class SignalTriggerExecution {
  private String workflowId;
  private String triggerUuid;
  private Map<String, Long> signalIds;
  private String condition;
  private String dedupExpr;
  private Map<String, ParamDefinition> params;
}
