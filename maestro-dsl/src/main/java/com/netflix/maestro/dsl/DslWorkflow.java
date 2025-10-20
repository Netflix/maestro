package com.netflix.maestro.dsl;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.dsl.jobs.Job;
import com.netflix.maestro.utils.Checks;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;

/** DSL data model representation of a workflow definition. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class DslWorkflow {
  private String id;
  private String name;
  private String description;
  private String runStrategy;
  private Long workflowConcurrency;
  private List<String> tags;
  private String timeout;
  private String criticality;
  private List<Job> jobs;

  private Map<String, Object> workflowParams = new LinkedHashMap<>();

  /** reserved fields cannot be set within workflowParams. */
  private static final Set<String> RESERVED_FIELDS =
      Set.of(
          "id",
          "name",
          "description",
          "run_strategy",
          "workflow_concurrency",
          "tags",
          "timeout",
          "jobs",
          "criticality",
          "workflow_params");

  /** workflowParams includes workflow params. */
  @JsonAnyGetter
  public Map<String, Object> getWorkflowParams() {
    return workflowParams;
  }

  /** Add fields to workflowParams. */
  @JsonAnySetter
  public void add(String key, Object value) {
    Checks.checkTrue(
        !RESERVED_FIELDS.contains(key),
        "[%s] is a reserved field and cannot be set within workflow params",
        key);
    workflowParams.put(key, value);
  }
}
