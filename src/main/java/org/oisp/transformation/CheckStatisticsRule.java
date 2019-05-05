package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.subCollections.NormalizedStatisticsValues;

import java.util.List;


//For statistics rules there is no real fulfillment check per component in contrast to Basic Rules and TB Rules
//Nevertheless, this task is mainly to translate RuleObservations to RuleConditions
public class CheckStatisticsRule extends DoFn<List<RulesWithObservation>, KV<String, RuleWithRuleConditions>> {
    private List<RulesWithObservation> observationRulesList;

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
                    if (!rc.isStatistics()) {
                        continue;
                    }
                    if (rc.getComponentId().equals(observation.getCid())) {
                        RuleCondition mutableRuleCondition = new RuleCondition(rc);
                        Double sample = Double.valueOf(observation.getValue());
                        NormalizedStatisticsValues nSV = new NormalizedStatisticsValues(sample,
                                sample * sample, observation.getOn(), Double.valueOf(observation.getValue()));
                        mutableRuleCondition.setStatisticsValues(nSV);
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
