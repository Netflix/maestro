package com.netflix.maestro.dsl.parsers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;

public class ParamParserTest {

  @Test
  public void testParamParser() {
    Map<String, Object> dslParams = new LinkedHashMap<>();
    dslParams.put("plain_str", "foo");
    dslParams.put("!expr_str", "1 + 1");
    dslParams.put("typed_long<long>", 100L);
    dslParams.put("typed_double", 1.23);
    dslParams.put("typed_bool", true);
    dslParams.put("mode_param(constant)", "bar");
    dslParams.put("combo<long>(immutable)", 100L);
    dslParams.put("!expr_typed<long>(constant)", "1 * 2");
    dslParams.put("str_array1", List.of("a", "b", "c"));
    dslParams.put("str_array2", new String[] {"a", "b", "c"});
    dslParams.put("long_array1", List.of(1L, 2L, 3L));
    dslParams.put("long_array2", new long[] {1L, 2L, 3L});
    dslParams.put("double_array1", List.of(1.1, 1.2, 1.3));
    dslParams.put("double_array2", new double[] {1.1, 1.2, 1.3});
    dslParams.put("bool_array1", List.of(true, false, true));
    dslParams.put("bool_array2", new boolean[] {true, false, true});
    dslParams.put("typed_array<array<string>>", List.of("x", "y", "z"));
    dslParams.put("empty_long_array<array<long>>", List.of());
    dslParams.put("empty_default", List.of());
    dslParams.put("string_map", Map.of("k1", "v1", "k2", "v2"));
    dslParams.put("empty_map", Map.of());
    dslParams.put("map", Map.of("foo", Map.of("bar", 123L, "baz", "bat")));

    Map<String, ParamDefinition> result = ParamParser.parse(dslParams);
    assertEquals(22, result.size());

    assertEquals("foo", result.get("plain_str").asStringParamDef().getValue());
    assertEquals("1 + 1", result.get("expr_str").asStringParamDef().getExpression());
    assertEquals(Long.valueOf(100), result.get("typed_long").asLongParamDef().getValue());
    assertEquals(new BigDecimal("1.23"), result.get("typed_double").asDoubleParamDef().getValue());
    assertEquals(Boolean.TRUE, result.get("typed_bool").asBooleanParamDef().getValue());
    assertEquals(ParamMode.CONSTANT, result.get("mode_param").asStringParamDef().getMode());
    assertEquals(Long.valueOf(100), result.get("combo").asLongParamDef().getValue());
    assertEquals(ParamMode.IMMUTABLE, result.get("combo").asLongParamDef().getMode());
    assertEquals("1 * 2", result.get("expr_typed").asLongParamDef().getExpression());
    assertEquals(ParamMode.CONSTANT, result.get("expr_typed").asLongParamDef().getMode());
    assertArrayEquals(
        new String[] {"a", "b", "c"}, result.get("str_array1").asStringArrayParamDef().getValue());
    assertArrayEquals(
        new String[] {"a", "b", "c"}, result.get("str_array2").asStringArrayParamDef().getValue());
    assertArrayEquals(
        new long[] {1L, 2L, 3L}, result.get("long_array1").asLongArrayParamDef().getValue());
    assertArrayEquals(
        new long[] {1L, 2L, 3L}, result.get("long_array2").asLongArrayParamDef().getValue());
    assertArrayEquals(
        new BigDecimal[] {new BigDecimal("1.1"), new BigDecimal("1.2"), new BigDecimal("1.3")},
        result.get("double_array1").asDoubleArrayParamDef().getValue());
    assertArrayEquals(
        new boolean[] {true, false, true},
        result.get("bool_array1").asBooleanArrayParamDef().getValue());
    assertArrayEquals(
        new boolean[] {true, false, true},
        result.get("bool_array2").asBooleanArrayParamDef().getValue());
    assertArrayEquals(
        new String[] {"x", "y", "z"}, result.get("typed_array").asStringArrayParamDef().getValue());
    assertArrayEquals(new long[0], result.get("empty_long_array").asLongArrayParamDef().getValue());
    assertArrayEquals(
        new String[0], result.get("empty_default").asStringArrayParamDef().getValue());
    assertEquals("v1", result.get("string_map").asStringMapParamDef().getValue().get("k1"));
    assertEquals("v2", result.get("string_map").asStringMapParamDef().getValue().get("k2"));
    assertTrue(result.get("empty_map").asMapParamDef().getValue().isEmpty());
    assertEquals(1, result.get("map").asMapParamDef().getValue().size());
    assertEquals(
        Long.valueOf(123),
        result
            .get("map")
            .asMapParamDef()
            .getValue()
            .get("foo")
            .asMapParamDef()
            .getValue()
            .get("bar")
            .asLongParamDef()
            .getValue());
    assertEquals(
        "bat",
        result
            .get("map")
            .asMapParamDef()
            .getValue()
            .get("foo")
            .asMapParamDef()
            .getValue()
            .get("baz")
            .asStringParamDef()
            .getValue());
  }

  @Test
  public void testParseNullOrEmptyMap() {
    assertNull(ParamParser.parse(null));
    Map<String, ParamDefinition> result = ParamParser.parse(new LinkedHashMap<>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testInvalidCases() {
    Stream.<Map<String, Object>>of(
            Map.of("foo(invalid)", "bar"),
            Map.of("foo<unknown>", 123L),
            Map.of("num<long>", "invalid"),
            Map.of("longVal<long>", 1.5),
            Map.of("invalidType", List.of(new Object())))
        .forEach(
            dslParams -> {
              try {
                ParamParser.parse(dslParams);
                fail();
              } catch (MaestroValidationException e) {
                // expected
              }
            });
  }
}
