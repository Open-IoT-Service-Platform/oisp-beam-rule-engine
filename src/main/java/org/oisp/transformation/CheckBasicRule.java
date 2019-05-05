package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RulesWithObservation;
import org.oisp.rules.conditions.BasicConditionChecker;
import org.oisp.collection.RuleWithRuleConditions;

import java.util.List;
public class CheckBasicRule extends DoFn<List<RulesWithObservation>, KV<String, RuleWithRuleConditions>> {

    private List<RulesWithObservation> observationRulesList;
    //private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        observationRulesList = c.element();
        sendFulfillmentState(c);
    }

    void sendFulfillmentState(ProcessContext c) {
        for (RulesWithObservation rwo : observationRulesList) {
            for (Rule rule: rwo.getRules()) {
                Observation observation = rwo.getObservation();
                RuleWithRuleConditions mutableRWRC = new RuleWithRuleConditions(rule);
                for (int i = 0; i < rule.getConditions().size(); i++) {
                    RuleCondition rc = rule.getConditions().get(i);
                    Boolean condFulfillment;
                    if (rc.isTimebased() || rc.isStatistics()) {
                        continue;
                    }
                    if (rc.getComponentId().equals(observation.getCid())) {
                        if (new BasicConditionChecker(rc).isConditionFulfilled(observation)) {
                            condFulfillment = true;
                        } else {
                            condFulfillment = false;
                        }
                        RuleCondition mutableRuleCondition = new RuleCondition(rc);
                        mutableRuleCondition.setFulfilled(condFulfillment);
                        mutableRuleCondition.setObservation(observation);
                        mutableRWRC.addRC(i, mutableRuleCondition);
                    }
                }
                if (!mutableRWRC.getRcHash().isEmpty()) {
                    c.output(KV.of(mutableRWRC.getRule().getId(), mutableRWRC));
                }
            }
        }
    }
}
