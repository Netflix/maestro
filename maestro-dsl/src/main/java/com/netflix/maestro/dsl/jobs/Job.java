package com.netflix.maestro.dsl.jobs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

/** Job interface for polymorphic deserialization of different job types in a DSL workflow. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(name = "job", value = TypedJob.class),
  @JsonSubTypes.Type(name = "subworkflow", value = Subworkflow.class),
  @JsonSubTypes.Type(name = "foreach", value = Foreach.class),
  @JsonSubTypes.Type(name = "while", value = While.class)
})
public interface Job {
  /** Get job id. */
  String getId();

  /** Get job transition info (successors only). */
  List<String> getTransition();

  /** Set job transition info (successors only). */
  void setTransition(List<String> transition);
}
