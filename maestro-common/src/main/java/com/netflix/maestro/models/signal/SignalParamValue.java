package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.parameter.LongParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import lombok.EqualsAndHashCode;

/**
 * Contains a signal param value, which can be indexed for signal dependency and trigger. It
 * contains either a long number or a string value.
 *
 * @author jun-he
 */
@JsonDeserialize(using = SignalParamValue.SignalParamValueDeserializer.class)
@JsonSerialize(using = SignalParamValue.SignalParamValueSerializer.class)
@EqualsAndHashCode
public final class SignalParamValue {
  private final long longValue;
  @Nullable private final String stringValue;

  /**
   * This constructor should set exactly one value between longValue and stringValue, the other
   * being null.
   */
  private SignalParamValue(long longValue, String stringValue) {
    this.longValue = longValue;
    this.stringValue = stringValue;
  }

  /** Static method to create a new SignalParamValue from a long value. */
  public static SignalParamValue of(long value) {
    return new SignalParamValue(value, null);
  }

  /**
   * Static method to create a new SignalParamValue from a String value.
   *
   * @throws NullPointerException if input value is null.
   */
  public static SignalParamValue of(String value) {
    Checks.notNull(value, "SignalParamValue can not be initialized with a null String value.");
    return new SignalParamValue(0, value);
  }

  /** Returns true if the SignalParamValue is a long. */
  public boolean isLong() {
    return stringValue == null;
  }

  /**
   * Gets the String value of the SignalParamValue. Caller should check that isLong() is false
   * first.
   */
  public String getString() {
    return stringValue;
  }

  public Parameter toParam(String name) {
    if (isLong()) {
      return LongParameter.builder()
          .name(name)
          .value(longValue)
          .evaluatedResult(longValue)
          .evaluatedTime(System.currentTimeMillis())
          .build();
    } else {
      return StringParameter.builder()
          .name(name)
          .value(stringValue)
          .evaluatedResult(stringValue)
          .evaluatedTime(System.currentTimeMillis())
          .build();
    }
  }

  /**
   * Gets the long value of the SignalParamValue. Caller should check that isLong() is true first.
   */
  public long getLong() {
    return longValue;
  }

  @Override
  public String toString() {
    if (isLong()) {
      return String.valueOf(longValue);
    }
    return '"' + stringValue + '"';
  }

  /** Deserializer with the validation check. */
  static class SignalParamValueDeserializer extends JsonDeserializer<SignalParamValue> {
    @Override
    public SignalParamValue deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isNumber() || value.isTextual(),
          "SignalParamValue value only supports Long or String. It does not support the type [%s]",
          value.getClass());
      if (value.isNumber()) {
        return SignalParamValue.of(value.asLong());
      }
      return SignalParamValue.of(value.asText());
    }
  }

  /** Serializer for ParsableLong. */
  static class SignalParamValueSerializer extends JsonSerializer<SignalParamValue> {
    @Override
    public void serialize(SignalParamValue value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (value.isLong()) {
        gen.writeNumber(value.getLong());
      } else {
        gen.writeString(value.getString());
      }
    }
  }
}
