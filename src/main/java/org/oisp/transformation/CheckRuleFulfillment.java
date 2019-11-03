package org.oisp.transformation;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.rules.ConditionOperators;

import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;


public class CheckRuleFulfillment extends DoFn<KV<String, RuleWithRuleConditions>, Rule> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        RuleWithRuleConditions rwrc = c.element().getValue();
        //check whether rule is fulfilled
        //if fulfilled, forward it to the Alert module
        Rule mutableRule = new Rule(rwrc.getRule());
        mutableRule.setConditions(new ArrayList<RuleCondition>());
        Map<Integer, RuleCondition> ruleConditionMap = rwrc.getRcHash();
        if (mutableRule.getConditionOperator() == ConditionOperators.AND) {
            Boolean result = true;
            for (int i = 0; i < rwrc.getRule().getConditions().size(); i++) {
                RuleCondition currentRc = ruleConditionMap.get(i);
                if (currentRc != null  && currentRc.getFulfilled()) {
                    mutableRule.getConditions().add(currentRc);
                } else {
                    result = false;
                    break;
                }
            }
            if (result) {
                c.output(mutableRule);
            }
        } else {
            Boolean result = false;
            for (SortedMap.Entry<Integer, RuleCondition> entry : ruleConditionMap.entrySet()) {
                if (entry.getValue().getFulfilled()) {
                    result |= true;
                    mutableRule.getConditions().add(entry.getValue());
                }
            }
            if (result) {
                System.out.printf("Rule %s triggered\n", mutableRule.getId());
                c.output(mutableRule);
            }
        }
    }
}
