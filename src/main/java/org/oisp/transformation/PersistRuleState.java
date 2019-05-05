package org.oisp.transformation;

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.collection.RuleCondition;
import java.util.Map;

@SuppressWarnings("PMD.UnusedPrivateField")
public class PersistRuleState extends DoFn<KV<String, RuleWithRuleConditions>, KV<String, RuleWithRuleConditions>> {


    @StateId("ruleCondHash")
    private final StateSpec<ValueState<RuleWithRuleConditions>> stateSpec = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("ruleCondHash") ValueState<RuleWithRuleConditions> state) { //NOPMD

        //Record all ruleconditions per Rule
        RuleWithRuleConditions rarc = c.element().getValue();
        Rule rule = rarc.getRule();
        RuleWithRuleConditions rch = state.read();
        if (rch == null) {
            state.write(new RuleWithRuleConditions(rarc));
            rch = state.read();
        }
        //update all RuleConditions
        for (Map.Entry<Integer, RuleCondition> entry : rarc.getRcHash().entrySet()) {
            Integer index = entry.getKey();
            rch.getRcHash().put(index, entry.getValue());
        }
        //send out all RuleConditions
        state.write(rch);
        c.output(KV.of(rule.getId(), rarc));
    }
}
