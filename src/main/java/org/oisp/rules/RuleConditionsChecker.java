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

package org.oisp.rules;

import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;

import java.util.function.Predicate;
import java.util.stream.Stream;

public class RuleConditionsChecker {

    private final Rule rule;

    public RuleConditionsChecker(Rule rule) {
        this.rule = rule;
    }

    public boolean isRuleFulfilledForComponent(String componentId, Predicate<RuleCondition> checkCondition) {
        Stream conditionsStream = rule.getConditionsForComponent(componentId).stream();
        if (rule.getConditionOperator() == ConditionOperators.AND) {
            return conditionsStream.allMatch(checkCondition);
        } else {
            return conditionsStream.anyMatch(checkCondition);
        }
    }
}
