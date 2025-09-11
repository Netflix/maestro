package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import lombok.Getter;

/** Maestro supported step types. */
public enum StepType {
  /** NoOp step. */
  NOOP("NoOp", true),
  /** Sleep step. */
  SLEEP("Sleep", true),
  /** Titus step. */
  TITUS("Titus", true),
  /** Notebook step. */
  NOTEBOOK("Notebook", true),
  /** Kubernetes step. */
  KUBERNETES("Kubernetes", true),
  /** Join step. */
  JOIN("Join", false),
  /** foreach loop step. */
  FOREACH("foreach", false),
  /** sequential while loop step. */
  WHILE("while", false),
  /** subworkflow step. */
  SUBWORKFLOW("subworkflow", false),
  /** template step. */
  TEMPLATE("template", false);

  private final String type;

  @JsonIgnore @Getter private final boolean leaf;

  @JsonValue
  public String getType() {
    return type;
  }

  StepType(String type, boolean leaf) {
    this.type = type;
    this.leaf = leaf;
  }

  /** Static creator. */
  @JsonCreator
  public static StepType create(String type) {
    return StepType.valueOf(type.toUpperCase(Locale.US));
  }
}
