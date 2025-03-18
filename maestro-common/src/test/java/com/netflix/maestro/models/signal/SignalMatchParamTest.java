package com.netflix.maestro.models.signal;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SignalMatchParam class.
 *
 * @author jun-he
 */
public class SignalMatchParamTest {
  @Test
  public void testIsSatisfiedForLong() {
    var val =
        SignalMatchParam.builder()
            .value(SignalParamValue.of(123))
            .operator(SignalOperator.GREATER_THAN)
            .build();
    Assert.assertFalse(val.isSatisfied(SignalParamValue.of(1)));
    Assert.assertTrue(val.isSatisfied(SignalParamValue.of(12345)));
  }

  @Test
  public void testIsSatisfiedForString() {
    var val =
        SignalMatchParam.builder()
            .value(SignalParamValue.of("foo"))
            .operator(SignalOperator.LESS_THAN)
            .build();
    Assert.assertFalse(val.isSatisfied(SignalParamValue.of("zzz")));
    Assert.assertTrue(val.isSatisfied(SignalParamValue.of("bar")));
  }

  @Test
  public void testIsSatisfiedForDifferentType() {
    var val =
        SignalMatchParam.builder()
            .value(SignalParamValue.of("foo"))
            .operator(SignalOperator.EQUALS_TO)
            .build();
    Assert.assertFalse(val.isSatisfied(SignalParamValue.of(1)));
  }
}
