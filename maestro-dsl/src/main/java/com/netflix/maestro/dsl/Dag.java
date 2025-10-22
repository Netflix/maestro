package com.netflix.maestro.dsl;

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
import com.netflix.maestro.utils.Checks;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Dag representation which can either be an order string or a transition map. Note that it does not
 * validate the dag structure.
 *
 * @param order either 'sequential' or 'parallel' order string.
 * @param transitions transition map for each job id
 */
@JsonDeserialize(using = Dag.DagDeserializer.class)
@JsonSerialize(using = Dag.DagSerializer.class)
public record Dag(@Nullable String order, @Nullable Map<String, List<String>> transitions) {

  public Dag {
    Checks.checkTrue(
        (order != null) ^ (transitions != null),
        "Either order or transitions must be set, but cannot be both.");
  }

  /** Deserializer for Dag. */
  @SuppressWarnings("unchecked")
  static class DagDeserializer extends JsonDeserializer<Dag> {
    @Override
    public Dag deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode value = p.getCodec().readTree(p);
      Checks.checkTrue(
          value.isObject() || value.isTextual(),
          "Dag value only supports an order string or a transition map. It does not support the type: "
              + value.getClass());
      if (value.isTextual()) {
        return new Dag(value.asText(), null);
      }
      return new Dag(null, p.getCodec().treeToValue(value, Map.class));
    }
  }

  /** Serializer for Dag. */
  static class DagSerializer extends JsonSerializer<Dag> {
    @Override
    public void serialize(Dag dag, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (dag.order() != null) {
        gen.writeString(dag.order());
      } else {
        gen.writeObject(dag.transitions());
      }
    }
  }
}
