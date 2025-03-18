package com.netflix.maestro.models.signal;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.models.parameter.BooleanParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import org.junit.Test;

/**
 * Tests for SignalMatchParamDef class.
 *
 * @author jun-he
 */
public class SignalMatchParamDefTest {
  @Test
  public void testBuildInvalidSignalMatchDef() {
    AssertHelper.assertThrows(
        "Not a valid SignalMatchParamDef",
        IllegalArgumentException.class,
        "The param and the operator in SignalMatchParamDef must be set",
        () -> SignalMatchParamDef.builder().operator(SignalOperator.EQUALS_TO).build());

    AssertHelper.assertThrows(
        "Not a valid SignalMatchParamDef",
        IllegalArgumentException.class,
        "The param and the operator in SignalMatchParamDef must be set",
        () ->
            SignalMatchParamDef.builder()
                .param(StringParamDefinition.builder().name("foo").build())
                .build());

    AssertHelper.assertThrows(
        "Not a valid SignalMatchParamDef",
        IllegalArgumentException.class,
        "type must be either LONG or STRING",
        () ->
            SignalMatchParamDef.builder()
                .param(BooleanParamDefinition.builder().name("foo").build())
                .operator(SignalOperator.EQUALS_TO)
                .build());
  }
}
