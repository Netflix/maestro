package com.netflix.maestro.models.signal;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SignalParamValue class.
 *
 * @author jun-he
 */
public class SignalParamValueTest {
  @Test
  public void testInvalid() {
    AssertHelper.assertThrows(
        "Not a valid SignalParamValue",
        NullPointerException.class,
        "SignalParamValue can not be initialized with a null String value",
        () -> SignalParamValue.of(null));
  }

  @Test
  public void testIsLong() {
    Assert.assertTrue(SignalParamValue.of(123).isLong());
    Assert.assertFalse(SignalParamValue.of("foo").isLong());
  }

  @Test
  public void testLongToParam() {
    Parameter param = SignalParamValue.of(123).toParam("foo");
    Assert.assertTrue(param.isEvaluated());
    Assert.assertTrue(param.isLiteral());
    Assert.assertEquals(ParamType.LONG, param.getType());
    Assert.assertEquals("foo", param.getName());
    Assert.assertEquals(123, param.asLongParam().getValue().longValue());
    Assert.assertEquals(123, param.asLongParam().getEvaluatedResult().longValue());
  }

  @Test
  public void testStringToParam() {
    Parameter param = SignalParamValue.of("bar").toParam("foo");
    Assert.assertTrue(param.isEvaluated());
    Assert.assertTrue(param.isLiteral());
    Assert.assertEquals(ParamType.STRING, param.getType());
    Assert.assertEquals("foo", param.getName());
    Assert.assertEquals("bar", param.asStringParam().getValue());
    Assert.assertEquals("bar", param.asStringParam().getEvaluatedResult());
  }
}
