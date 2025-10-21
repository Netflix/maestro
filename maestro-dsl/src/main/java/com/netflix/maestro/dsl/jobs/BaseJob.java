package com.netflix.maestro.dsl.jobs;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.FailureMode;
import com.netflix.maestro.utils.Checks;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public abstract class BaseJob implements Job {
  /** reserved fields cannot be set within jobParams. */
  static final Set<String> RESERVED_FIELDS =
      Set.of(
          "id",
          "name",
          "description",
          "failure_mode",
          "tags",
          "timeout",
          "transition",
          "job_params");

  private String id;
  private String name;
  private String description;
  private FailureMode failureMode;
  private List<String> tags;
  private String timeout;
  private List<String> transition;

  private Map<String, Object> jobParams = new LinkedHashMap<>();

  abstract Set<String> getReservedFields();

  /** jobParams includes job params. */
  @JsonAnyGetter
  public Map<String, Object> getJobParams() {
    return jobParams;
  }

  /** Add fields to jobParams. */
  @JsonAnySetter
  public void add(String key, Object value) {
    Checks.checkTrue(
        !getReservedFields().contains(key),
        "[%s] is a reserved field and cannot be set within job params",
        key);
    jobParams.put(key, value);
  }
}
