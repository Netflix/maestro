package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import lombok.Data;

/** Signals artifact to store dynamic signals generated at step runtime. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"outputs", "info"},
    alphabetic = true)
@Data
public class DynamicOutputArtifact implements Artifact {
  @Valid private Map<StepOutputsDefinition.StepOutputType, List<MapParameter>> outputs;
  @Nullable private TimelineLogEvent info;

  @JsonIgnore
  @Override
  public DynamicOutputArtifact asDynamicOutput() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.DYNAMIC_OUTPUT;
  }

  @JsonIgnore
  public @Nullable List<MapParameter> getOutputSignals() {
    if (outputs != null) {
      return outputs.get(StepOutputsDefinition.StepOutputType.SIGNAL);
    }
    return null;
  }
}
