package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.parameter.Parameter;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;

/** Request to create a step output data request. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"params", "artifacts"},
    alphabetic = true)
@Data
public class StepOutputDataRequest {
  @Valid private Map<String, Parameter> params;
  @Valid private Map<String, Artifact> artifacts;
  private Map<String, Object> extraInfo = new LinkedHashMap<>();

  /** Get fields from extraInfo. */
  @JsonAnyGetter
  public Map<String, Object> getExtraInfo() {
    return extraInfo;
  }

  /** Add fields to extraInfo. */
  @JsonAnySetter
  public void add(String name, Object value) {
    extraInfo.put(name, value);
  }
}
