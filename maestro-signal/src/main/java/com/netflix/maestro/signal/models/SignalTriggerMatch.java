package com.netflix.maestro.signal.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.signal.SignalInstance;
import lombok.Data;

/**
 * Data model for signal trigger matched with the given signal instance, will be passed over the
 * queue.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"workflow_id", "trigger_uuid", "signal_instance"},
    alphabetic = true)
@Data
public class SignalTriggerMatch {
  private String workflowId;
  private String triggerUuid;
  @Nullable private SignalInstance signalInstance; // the cleaned instance only carries params.
}
