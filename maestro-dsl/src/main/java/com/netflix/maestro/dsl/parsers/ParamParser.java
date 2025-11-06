package com.netflix.maestro.dsl.parsers;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.parameter.BooleanArrayParamDefinition;
import com.netflix.maestro.models.parameter.BooleanParamDefinition;
import com.netflix.maestro.models.parameter.DoubleArrayParamDefinition;
import com.netflix.maestro.models.parameter.DoubleParamDefinition;
import com.netflix.maestro.models.parameter.LongArrayParamDefinition;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.StringArrayParamDefinition;
import com.netflix.maestro.models.parameter.StringMapParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.MapHelper;
import com.netflix.maestro.utils.ParamHelper;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Parser class for parsing DSL param to Maestro param definitions. */
final class ParamParser {
  private static final String ARRAY_PREFIX = "ARRAY<";
  private static final int ARRAY_PREFIX_OFFSET = ARRAY_PREFIX.length();
  private static final String PARAM_MAP_KEY = "PMAP";
  private static final String STR_PARAM_KEY = "MAP";

  private ParamParser() {}

  /**
   * paramInfo containing the param name, type, mode, and literal flag.
   *
   * @param name param name excluding type/mode syntax
   * @param isLiteral true if literal value, false if expression
   * @param type param type
   * @param mode param mode
   */
  private record ParamInfo(
      String name, boolean isLiteral, ParamType type, @Nullable ParamMode mode) {}

  /**
   * Convert a Map of raw objects to a Map of ParamDefinitions.
   *
   * @param dslParams the raw param map
   * @return map of ParamDefinitions
   */
  static Map<String, ParamDefinition> parse(Map<String, Object> dslParams) {
    if (dslParams == null) {
      return null;
    }

    return dslParams.entrySet().stream()
        .map(entry -> convertObjectToParamDefinition(entry.getKey(), entry.getValue()))
        .collect(
            MapHelper.toListMap(
                ParamDefinition::getName, // Use cleaned name as map key
                paramDef -> paramDef)); // The param definition itself as value
  }

  /**
   * Convert an object to a ParamDefinition.
   *
   * @param name the parameter name
   * @param value the parameter value
   * @return the corresponding ParamDefinition
   */
  private static ParamDefinition convertObjectToParamDefinition(String name, Object value) {
    Checks.notNull(name, "param name cannot be null");
    Checks.notNull(value, "param value for param [%s] cannot be null", name);

    ParamInfo info = extractParamInfo(name.trim(), value);

    return switch (info.type()) {
      case STRING -> buildStringParamDefinition(info, value);
      case LONG -> buildLongParamDefinition(info, value);
      case DOUBLE -> buildDoubleParamDefinition(info, value);
      case BOOLEAN -> buildBooleanParamDefinition(info, value);
      case STRING_ARRAY -> buildStringArrayParamDefinition(info, value);
      case LONG_ARRAY -> buildLongArrayParamDefinition(info, value);
      case DOUBLE_ARRAY -> buildDoubleArrayParamDefinition(info, value);
      case BOOLEAN_ARRAY -> buildBooleanArrayParamDefinition(info, value);
      case STRING_MAP -> buildStringMapParamDefinition(info, value);
      case MAP -> buildMapParamDefinition(info, value);
    };
  }

  /**
   * Parse a parameter name that may contain type and mode information.
   *
   * <p>Supported formats:
   *
   * <ul>
   *   <li>{@code foo} - plain parameter name
   *   <li>{@code !foo} - expression parameter (SEL expression)
   *   <li>{@code foo<long>} - parameter with type specification
   *   <li>{@code foo<long>(constant)} - parameter with type and mode
   *   <li>{@code !foo<string>} - expression parameter with type
   *   <li>{@code foo<array<long>>} - array parameter with element type
   *   <li>{@code foo<map>} - string map parameter
   *   <li>{@code foo<pmap>} - map parameter
   * </ul>
   *
   * @param name the parameter name to parse
   * @return paramInfo containing the param name, type, mode, and literal flag
   */
  private static ParamInfo extractParamInfo(String name, Object value) {
    String paramName = name;
    boolean isLiteral = true;
    ParamType type = null;
    ParamMode mode = null;

    if (paramName.startsWith("!")) { // '!' indicates a SEL expression
      isLiteral = false;
      paramName = paramName.substring(1);
    }

    // Parse mode based on dsl syntax using parentheses: (constant), (provided), etc.
    int modeStart = paramName.lastIndexOf('(');
    int modeEnd = paramName.lastIndexOf(')');
    if (modeStart > 0 && modeEnd > modeStart) {
      String modeStr = paramName.substring(modeStart + 1, modeEnd).trim();
      try {
        mode = ParamMode.create(modeStr);
      } catch (IllegalArgumentException e) {
        throw new MaestroValidationException(
            e,
            "Invalid param mode [%s] in param name [%s]. Supported modes are [%s]",
            modeStr,
            name,
            Arrays.toString(ParamMode.values()));
      }
      paramName = paramName.substring(0, modeStart).trim();
    }

    // Parse type based on dsl syntax using brackets: <long>, <string>, <array<long>>, etc.
    int typeStart = paramName.indexOf('<');
    int typeEnd = paramName.lastIndexOf('>');
    if (typeStart > 0 && typeEnd > typeStart) {
      String typeStr = paramName.substring(typeStart + 1, typeEnd).trim().toUpperCase(Locale.US);
      type = parseParamType(typeStr);
      paramName = paramName.substring(0, typeStart).trim();
    }
    Checks.checkTrue(
        !paramName.isEmpty(), "param name cannot be empty after parsing name: [%s]", name);

    if (isLiteral) {
      ParamType impliedType = deriveParamType(value);
      if (impliedType == null && type == null) {
        type = ParamType.STRING_ARRAY; // default to string array for empty list
      }
      if (type == null) { // derive implied type from value object
        type = impliedType;
      } else if (impliedType != null && type != impliedType) {
        if (type == ParamType.MAP && impliedType == ParamType.STRING_MAP) {
          // allow string map to be treated as param map
        } else {
          throw new MaestroValidationException(
              "Type mismatch for param [%s]: specified type [%s] does not match implied type [%s] from its value: [%s]",
              name, type, impliedType, value);
        }
      }
    } else {
      if (type == null) { // For SEL expressions, if no type is specified, default to STRING
        type = ParamType.STRING;
      }
    }

    return new ParamInfo(paramName, isLiteral, type, mode);
  }

  /**
   * Parse a type string to ParamType enum. Handles simple types like "long", "string" and complex
   * types like "array<long>", "pmap".
   *
   * @param typeStr the type string to parse
   * @return the ParamType enum
   */
  private static ParamType parseParamType(String typeStr) {
    if (typeStr.startsWith(ARRAY_PREFIX) && typeStr.endsWith(">")) { // handle array type
      String elementType = typeStr.substring(ARRAY_PREFIX_OFFSET, typeStr.length() - 1).trim();
      return switch (elementType) {
        case "STRING" -> ParamType.STRING_ARRAY;
        case "LONG" -> ParamType.LONG_ARRAY;
        case "DOUBLE" -> ParamType.DOUBLE_ARRAY;
        case "BOOLEAN" -> ParamType.BOOLEAN_ARRAY;
        default ->
            throw new MaestroValidationException(
                "Unsupported type [%s]. Supported array element types are STRING, LONG, DOUBLE, BOOLEAN",
                typeStr);
      };
    } else if (PARAM_MAP_KEY.equals(typeStr)) { // pmap is parameter map
      return ParamType.MAP;
    } else if (STR_PARAM_KEY.equals(typeStr)) { // map is a simple string map
      return ParamType.STRING_MAP;
    }

    try { // all other cases
      return ParamType.valueOf(typeStr);
    } catch (IllegalArgumentException e) {
      throw new MaestroValidationException(
          e,
          "Invalid param type [%s]. Supported types are [%s]",
          typeStr,
          Arrays.toString(ParamType.values()));
    }
  }

  /**
   * Derive the implied param type from the param value.
   *
   * @param value the parameter value
   * @return the ParamType enum
   */
  private static ParamType deriveParamType(Object value) {
    return switch (value) {
      case String ignored -> ParamType.STRING;
      case Long ignored -> ParamType.LONG;
      case Double ignored -> ParamType.DOUBLE;
      case Boolean ignored -> ParamType.BOOLEAN;
      case Map<?, ?> mapVal -> {
        if (mapVal.isEmpty()) {
          yield ParamType.MAP; // default to param map
        } else {
          if (mapVal.values().stream().allMatch(v -> v instanceof String)) {
            yield ParamType.STRING_MAP;
          } else {
            yield ParamType.MAP;
          }
        }
      }
      case List<?> listVal -> {
        if (listVal.isEmpty()) {
          yield null; // cannot derive type from an empty list
        } else {
          yield switch (listVal.getFirst()) {
            case String ignored -> ParamType.STRING_ARRAY;
            case Long ignored -> ParamType.LONG_ARRAY;
            case Double ignored -> ParamType.DOUBLE_ARRAY;
            case Boolean ignored -> ParamType.BOOLEAN_ARRAY;
            default ->
                throw new MaestroValidationException("Invalid type for array param: [%s]", value);
          };
        }
      }
      case String[] ignored -> ParamType.STRING_ARRAY;
      case long[] ignored -> ParamType.LONG_ARRAY;
      case double[] ignored -> ParamType.DOUBLE_ARRAY;
      case boolean[] ignored -> ParamType.BOOLEAN_ARRAY;
      default -> throw new MaestroValidationException("Invalid type for param value: [%s]", value);
    };
  }

  private static ParamDefinition buildStringParamDefinition(ParamInfo info, Object value) {
    var builder = StringParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value((String) value);
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildLongParamDefinition(ParamInfo info, Object value) {
    var builder = LongParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value((Long) value);
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildDoubleParamDefinition(ParamInfo info, Object value) {
    var builder = DoubleParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value(new BigDecimal(String.valueOf(value)));
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildBooleanParamDefinition(ParamInfo info, Object value) {
    var builder = BooleanParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value((Boolean) value);
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static ParamDefinition buildStringArrayParamDefinition(ParamInfo info, Object value) {
    var builder = StringArrayParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      if (value instanceof List<?> listVal) {
        builder.value(((List<String>) listVal).toArray(new String[0]));
      } else { // default to array case and might fail
        builder.value((String[]) value);
      }
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildLongArrayParamDefinition(ParamInfo info, Object value) {
    var builder = LongArrayParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      if (value instanceof List<?> listVal) {
        builder.value(listVal.stream().mapToLong(i -> ((Number) i).longValue()).toArray());
      } else { // default to array case and might fail
        builder.value((long[]) value);
      }
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildDoubleArrayParamDefinition(ParamInfo info, Object value) {
    var builder = DoubleArrayParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value(ParamHelper.toDecimalArray(info.name, value));
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  private static ParamDefinition buildBooleanArrayParamDefinition(ParamInfo info, Object value) {
    var builder = BooleanArrayParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      if (value instanceof List<?> listVal) {
        boolean[] boolArray = new boolean[listVal.size()];
        for (int i = 0; i < listVal.size(); i++) {
          boolArray[i] = (Boolean) listVal.get(i);
        }
        builder.value(boolArray);
      } else { // default to array case and might fail
        builder.value((boolean[]) value);
      }
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static ParamDefinition buildStringMapParamDefinition(ParamInfo info, Object value) {
    var builder = StringMapParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      builder.value((Map<String, String>) value);
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static ParamDefinition buildMapParamDefinition(ParamInfo info, Object value) {
    var builder = MapParamDefinition.builder().name(info.name()).mode(info.mode());
    if (info.isLiteral()) {
      Map<String, Object> objectMap = (Map<String, Object>) value;
      Map<String, ParamDefinition> nestedParams = parse(objectMap);
      builder.value(nestedParams);
    } else {
      builder.expression(value.toString());
    }
    return builder.build();
  }
}
