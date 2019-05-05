package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.collection.RulesWithObservation;
import org.oisp.rules.conditions.ConditionFunctionChecker;

import java.util.List;
import java.util.TreeMap;

public class CheckTimeBasedRule extends DoFn<List<RulesWithObservation>, KV<String, RuleWithRuleConditions>> {
    private List<RulesWithObservation> observationRulesList;
    //private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        observationRulesList = c.element();
        sendFulfillmentState(c);
    }

    void sendFulfillmentState(ProcessContext c) {
        for (RulesWithObservation rwo : observationRulesList) {
            for (Rule rule : rwo.getRules()) {
                RuleWithRuleConditions  mutableRWRC = new RuleWithRuleConditions(rule);
                Observation observation = rwo.getObservation();
                for (int i = 0; i < rule.getConditions().size(); i++) {
                    RuleCondition rc = rule.getConditions().get(i);
                    if (!rc.isTimebased()) {
                        continue;
                    }
                    if (rc.getComponentId().equals(observation.getCid())) {
                        boolean result = false;
                        if (new ConditionFunctionChecker(rc).isConditionFulfilled(observation.getValue())) {
                            result = true;
                        } else {
                            result = false;
                        }
                        RuleCondition mutableRuleCondition = new RuleCondition(rc);
                        mutableRuleCondition.setObservation(observation);
                        mutableRuleCondition.setTimeBasedState(new TreeMap<Long, Boolean>());
                        mutableRuleCondition.getTimeBasedState().put(observation.getOn(), result);
                        mutableRWRC.addRC(i, mutableRuleCondition);
                    }
                }
                if (!mutableRWRC.getRcHash().isEmpty()) {
                    c.output(KV.of(rule.getId(), mutableRWRC));
                }
            }
        }
    }
}
