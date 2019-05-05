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

package org.oisp.rules.conditions;

import org.oisp.rules.DataType;
import org.oisp.rules.Operators;
import org.oisp.rules.conditions.functions.Functions;
import org.oisp.rules.conditions.functions.FunctionsFactory;
import org.oisp.rules.conditions.functions.IllegalOperatorException;
import org.oisp.collection.RuleCondition;

import java.util.List;


public class ConditionFunctionChecker {

    private final Operators operator;
    private final List<String> values;
    private final DataType componentDataType;

    private Functions functions;

    public ConditionFunctionChecker(RuleCondition ruleCondition) {
        this(ruleCondition.getOperator(), ruleCondition.getValues(), ruleCondition.getComponentDataType());
    }

    public ConditionFunctionChecker(Operators operator, List<String> values, DataType componentDataType) {
        this.operator = operator;
        this.values = values;
        this.componentDataType = componentDataType;
    }

    public boolean isConditionFulfilled(String value) {
        try {
            functions = FunctionsFactory.getFunctions(componentDataType);
            return isFulfilled(value);
        } catch (IllegalOperatorException e) {
            return false;
        }
    }

    private boolean isFulfilled(String value) {
        return operator.isFulfilled(functions, value, values);
    }
}
