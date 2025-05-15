package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.utils.Checks;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Signal match param def represents a param definition from the signal and then gets indexed by
 * maestro signal module. The workflow trigger or step dependency can apply {@link SignalOperator}
 * on it. Its value only support long or string type.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"param", "operator"},
    alphabetic = true)
@JsonDeserialize(builder = SignalMatchParamDef.SignalMatchParamDefBuilder.class)
@Builder
@ToString
@EqualsAndHashCode
@Getter
public class SignalMatchParamDef {
  private final ParamDefinition param;
  private final SignalOperator operator;

  /** Builder class for {@link SignalMatchParamDef}. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class SignalMatchParamDefBuilder {
    /** build method with param type check. */
    public SignalMatchParamDef build() {
      Checks.checkTrue(
          this.param != null && operator != null,
          "The param and the operator in SignalMatchParamDef must be set");
      Checks.checkTrue(
          this.param.getType() == ParamType.STRING || this.param.getType() == ParamType.LONG,
          "SignalMatchParamDef param [%s]'s type must be either LONG or STRING",
          this.param);
      return new SignalMatchParamDef(this.param, this.operator);
    }
  }
}
