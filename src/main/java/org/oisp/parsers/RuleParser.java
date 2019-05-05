/*
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.oisp.parsers;

import org.oisp.apiclients.rules.model.ComponentRulesResponse;
import org.oisp.apiclients.rules.model.ConditionValue;
import org.oisp.apiclients.rules.model.Conditions;
import org.oisp.apiclients.rules.model.RuleResponse;
import org.oisp.rules.ConditionOperators;
import org.oisp.rules.ConditionType;
import org.oisp.rules.DataType;
import org.oisp.rules.Operators;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.io.Serializable;

public class RuleParser implements Serializable {

    private final List<ComponentRulesResponse> componentsRulesResponse;

    public RuleParser(List<ComponentRulesResponse> componentsRulesResponse) {
        this.componentsRulesResponse = componentsRulesResponse;
    }

    public Map<String, List<Rule>> getComponentRules() {
        Map<String, List<Rule>> result = new HashMap<>();

        componentsRulesResponse.forEach(componentRules -> {
            result.put(componentRules.getComponentId(), getRules(componentRules));

        });
        return result;
    }

    public List<Rule> getRules(ComponentRulesResponse componentRules) {
        return componentRules.getRules().stream()
                .map(rule -> toRule(rule))
                .collect(Collectors.toList());
    }

    private Rule toRule(RuleResponse response) {
        Rule rule = new Rule();
        rule.setAccountId(response.getDomainId());
        rule.setId(response.getId());
        rule.setConditions(toRuleConditions(rule.getId(), response.getConditions()));
        rule.setConditionOperator(ConditionOperators.fromString(response.getConditions().getOperator()));
        rule.setStatus(response.getStatus());
        return rule;
    }

    private List<RuleCondition> toRuleConditions(String ruleId, Conditions responseConditions) {
        return responseConditions.getValues().stream()
                .map(condition -> toRuleCondition(ruleId, condition))
                .collect(Collectors.toList());
    }

    private RuleCondition toRuleCondition(String ruleId, ConditionValue condition) {
        RuleCondition ruleCondition = new RuleCondition();
        ruleCondition.setComponentId(condition.getComponent().getCid());
        ruleCondition.setComponentDataType(DataType.valueOf(condition.getComponent().getDataType().toUpperCase()));
        ruleCondition.setOperator(Operators.fromString(condition.getOperator()));
        ruleCondition.setType(ConditionType.valueOf(condition.getType().toUpperCase()));
        ruleCondition.setValues(condition.getValues());
        ruleCondition.setTimeLimit(condition);
        ruleCondition.setRuleId(ruleId);
        ruleCondition.setMinimalObservationCountInTimeWindow(condition.getBaselineMinimalInstances());

        return ruleCondition;
    }
}
