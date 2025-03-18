package com.netflix.maestro.validations;

import com.netflix.maestro.models.signal.SignalMatchParam;
import com.netflix.maestro.models.signal.SignalOperator;
import com.netflix.maestro.models.signal.SignalParamValue;
import com.netflix.maestro.models.trigger.SignalTrigger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for SignalTriggerConstraint class.
 *
 * @author jun-he
 */
public class SignalTriggerConstraintTest extends BaseConstraintTest {
  private static class DummyWorkflow {
    List<@SignalTriggerConstraint SignalTrigger> triggers;

    DummyWorkflow(SignalTrigger trigger) {
      triggers = List.of(trigger);
    }
  }

  @Test
  public void testValid() {
    SignalTrigger trigger1 = new SignalTrigger();
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger1)).isEmpty());

    SignalTrigger trigger2 = new SignalTrigger();
    trigger2.setDefinitions(Map.of());
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger2)).isEmpty());

    SignalTrigger trigger3 = new SignalTrigger();
    trigger3.setDefinitions(Map.of("signal_a", new SignalTrigger.SignalTriggerEntry()));
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger3)).isEmpty());

    SignalTrigger trigger4 = new SignalTrigger();
    var entry4 = new SignalTrigger.SignalTriggerEntry();
    entry4.setJoinKeys(new String[] {"foo"});
    trigger4.setDefinitions(Map.of("signal_a", entry4));
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger4)).isEmpty());

    SignalTrigger trigger5 = new SignalTrigger();
    var entry5 = new SignalTrigger.SignalTriggerEntry();
    entry5.setMatchParams(
        Map.of(
            "bar",
            SignalMatchParam.builder()
                .value(SignalParamValue.of(123))
                .operator(SignalOperator.EQUALS_TO)
                .build()));
    trigger5.setDefinitions(Map.of("signal_a", entry5));
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger5)).isEmpty());

    SignalTrigger trigger6 = new SignalTrigger();
    var entry6 = new SignalTrigger.SignalTriggerEntry();
    entry6.setJoinKeys(new String[] {"foo"});
    entry6.setMatchParams(
        Map.of(
            "bar",
            SignalMatchParam.builder()
                .value(SignalParamValue.of(123))
                .operator(SignalOperator.EQUALS_TO)
                .build()));
    trigger6.setDefinitions(Map.of("signal_a", entry6));
    Assert.assertTrue(validator.validate(new DummyWorkflow(trigger6)).isEmpty());
  }

  @Test
  public void testInValidWhenSignalNameSizeOverLimit() {
    SignalTrigger trigger = new SignalTrigger();
    trigger.setDefinitions(
        Map.of(
            "signal_a",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_b",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_c",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_d",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_e",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_f",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_g",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_h",
            new SignalTrigger.SignalTriggerEntry(),
            "signal_i",
            new SignalTrigger.SignalTriggerEntry()));
    Set<ConstraintViolation<DummyWorkflow>> violations =
        validator.validate(new DummyWorkflow(trigger));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "[signal-trigger] the signal names within the signal triggers are more than the limit [8]",
        violations.iterator().next().getMessage());
  }

  @Test
  public void testInValidWhenJoinKeysWithDifferentSize() {
    SignalTrigger trigger = new SignalTrigger();
    var entry1 = new SignalTrigger.SignalTriggerEntry();
    entry1.setJoinKeys(new String[] {"foo"});
    var entry2 = new SignalTrigger.SignalTriggerEntry();
    entry2.setJoinKeys(new String[] {"foo", "bar"});
    trigger.setDefinitions(Map.of("signal_a", entry1, "signal_b", entry2));
    Set<ConstraintViolation<DummyWorkflow>> violations =
        validator.validate(new DummyWorkflow(trigger));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "[signal-trigger] the join_keys lengths between signals in the signal triggers must be the same",
        violations.iterator().next().getMessage());
  }

  @Test
  public void testInValidWhenJoinKeysUsedForMatch() {
    SignalTrigger trigger = new SignalTrigger();
    var entry = new SignalTrigger.SignalTriggerEntry();
    entry.setJoinKeys(new String[] {"bar"});
    entry.setMatchParams(
        Map.of(
            "bar",
            SignalMatchParam.builder()
                .value(SignalParamValue.of(123))
                .operator(SignalOperator.EQUALS_TO)
                .build()));
    trigger.setDefinitions(Map.of("signal_a", entry));
    Set<ConstraintViolation<DummyWorkflow>> violations =
        validator.validate(new DummyWorkflow(trigger));
    Assert.assertEquals(1, violations.size());
    Assert.assertEquals(
        "[signal-trigger] the join_key [bar] cannot be used in match_params at the same time",
        violations.iterator().next().getMessage());
  }
}
