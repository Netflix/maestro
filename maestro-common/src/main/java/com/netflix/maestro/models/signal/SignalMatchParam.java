package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Signal match param represents a param from the signal and then gets indexed by maestro signal
 * module. The workflow trigger or step dependency can apply {@link SignalOperator} on it. Its value
 * only support long or string type. It does not save the param name.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"value", "operator"},
    alphabetic = true)
@JsonDeserialize(builder = SignalMatchParam.SignalMatchParamBuilder.class)
@Builder
@ToString
@EqualsAndHashCode
@Getter
public class SignalMatchParam {
  private SignalParamValue value;
  private SignalOperator operator;

  @JsonIgnore
  public boolean isSatisfied(SignalParamValue input) {
    if (value.isLong() != input.isLong()) {
      return false;
    }
    if (value.isLong()) {
      return operator.apply(input.getLong(), value.getLong());
    } else {
      return operator.apply(input.getString(), value.getString());
    }
  }

  /** Builder class for {@link SignalMatchParam}. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class SignalMatchParamBuilder {}
}
