package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.validations.MaestroNameConstraint;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;
import lombok.ToString;

/**
 * Request for external API requests to create a Maestro signal instance.
 *
 * @author jun-he
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"name", "params", "payload", "request_id", "request_time"},
    alphabetic = true)
@Data
@ToString
public class SignalCreateRequest {
  /** Name of the signal. */
  @MaestroNameConstraint private String name;

  /** Indexed parameters used for matching, only support string and long values. */
  @Nullable private Map<String, SignalParamValue> params;

  /**
   * Map of key value pairs stored in the payload of the signal instance, which are not indexed for
   * matching.
   */
  @Nullable private Map<String, Object> payload = new LinkedHashMap<>();

  /** Unique id provided by the users. If unset, system puts an uuid based on signal. */
  @Nullable private String requestId;

  /** The time that the signal is announced. By default, it is request received time. */
  private long requestTime = System.currentTimeMillis();

  /**
   * Get the signal's payload.
   *
   * @return the un-indexed payload of signal request.
   */
  @JsonAnyGetter
  public Map<String, Object> getPayload() {
    return this.payload;
  }

  /**
   * Add a payload field to signal's payload.
   *
   * @param key the payload field name, for example "source".
   * @param value the value of the payload field, for example a workflow step instance reference
   *     info.
   */
  @JsonAnySetter
  public void addPayload(String key, Object value) {
    this.payload.put(key, value);
  }
}
