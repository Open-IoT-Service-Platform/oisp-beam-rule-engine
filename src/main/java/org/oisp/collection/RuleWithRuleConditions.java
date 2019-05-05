package org.oisp.collection;


import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

public class RuleWithRuleConditions implements Serializable {

    private final Rule rule;
    private SortedMap<Integer, RuleCondition> rcHash;

    public RuleWithRuleConditions() {
        rule = null;
        rcHash = new TreeMap<Integer, RuleCondition>();
    }
    public RuleWithRuleConditions(Rule rule) {
        this.rule = rule;
        rcHash = new TreeMap<Integer, RuleCondition>();
    }

    public RuleWithRuleConditions(RuleWithRuleConditions other) {
        rule = other.rule;
        rcHash = new TreeMap<Integer, RuleCondition>(other.rcHash);
    }

    public void addRC(Integer index, RuleCondition rc) {
        rcHash.put(index, rc);
    }
    public SortedMap<Integer, RuleCondition> getRcHash() {
        return rcHash;
    }

    public void setRcHash(SortedMap<Integer, RuleCondition> rcHash) {
        this.rcHash = rcHash;
    }

    public Rule getRule() {

        return rule;
    }
}
