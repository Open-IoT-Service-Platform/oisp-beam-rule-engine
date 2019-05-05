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

package org.oisp.collection;

import com.google.common.base.Objects;
import org.oisp.apiclients.rules.model.ConditionValue;
import org.oisp.collection.subCollections.NormalizedStatisticsValues;
import org.oisp.rules.ConditionType;
import org.oisp.rules.DataType;
import org.oisp.rules.Operators;

import java.io.Serializable;
import java.util.List;
import java.util.SortedMap;


public class RuleCondition implements Serializable {


    private List<String> values;
    private Operators operator;
    private ConditionType type;
    private String componentId;
    private DataType componentDataType;
    private Boolean fulfilled;

    /**
     * timeLimit for timebased and statistics conditions (in seconds)
     */
    private Long timeLimit;
    private String ruleId;
    // For Statistics rule: only trigger when enough samples are found
    private Long minimalObservationCountInTimeWindow;
    // Observation which triggered fulfillment of the condition
    private Observation observation;
    // ONLY for TB Rules: contains the timeBased max found timemstamp
    // difference of fulfilling elements
    private SortedMap<Long, Boolean> timeBasedState;
    //ONLY for Statistics Rule: Values to decide when to trigger
    private NormalizedStatisticsValues statisticsValues;

    public NormalizedStatisticsValues getStatisticsValues() {
        return statisticsValues;
    }

    public void setStatisticsValues(NormalizedStatisticsValues statisticsValues) {
        this.statisticsValues = statisticsValues;
    }

    public SortedMap<Long, Boolean> getTimeBasedState() {
        return timeBasedState;
    }

    public void setTimeBasedState(SortedMap<Long, Boolean> timeBasedState) {
        this.timeBasedState = timeBasedState;
    }

    public RuleCondition() {
        fulfilled = false;

    }

    public RuleCondition(RuleCondition other) {
        values = other.values;
        operator = other.operator;
        type = other.type;
        componentId = other.componentId;
        componentDataType = other.componentDataType;
        fulfilled = other.fulfilled;
        timeLimit = other.timeLimit;
        ruleId = other.ruleId;
        minimalObservationCountInTimeWindow = other.minimalObservationCountInTimeWindow;
        observation = other.observation;
    }

    public RuleCondition copy() {
        RuleCondition rc = new RuleCondition(this);
        if (getTimeBasedState() != null) {
            rc.setTimeBasedState(getTimeBasedState());
        }
        if (statisticsValues != null) {
            rc.setStatisticsValues(new NormalizedStatisticsValues(getStatisticsValues()));
        }
        return rc;
    }

    public void setTimeLimit(Long timeLimit) {
        this.timeLimit = timeLimit;
    }

    public Observation getObservation() {
        return observation;
    }

    public void setObservation(Observation observation) {
        this.observation = observation;
    }

    public Boolean getFulfilled() {
        return fulfilled;
    }

    public void setFulfilled(Boolean fulfilled) {
        this.fulfilled = fulfilled;
    }

    public Long getMinimalObservationCountInTimeWindow() {
        return minimalObservationCountInTimeWindow;
    }

    public void setMinimalObservationCountInTimeWindow(Long minimalObservationCountInTimeWindow) {
        this.minimalObservationCountInTimeWindow = minimalObservationCountInTimeWindow;
    }

    public boolean isTimebased() {
        return getType() == ConditionType.TIME;
    }

    public boolean isStatistics() {
        return getType() == ConditionType.STATISTICS;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public Long getTimeLimit() {
        return timeLimit;
    }

    public void setTimeLimit(ConditionValue conditionValue) {
        if (getType() == null) {
            throw new IllegalStateException("Condition Type must be set before setting timeLimit");
        }
        if (isTimebased()) {
            this.timeLimit = conditionValue.getTimeLimit();
        } else if (isStatistics()) {
            this.timeLimit = conditionValue.getBaselineSecondsBack();
        }
    }

    public void setTimeLimit(long timeLimit) {
        this.timeLimit = timeLimit;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public Operators getOperator() {
        return operator;
    }

    public void setOperator(Operators operator) {
        this.operator = operator;
    }

    public ConditionType getType() {
        return type;
    }

    public void setType(ConditionType type) {
        this.type = type;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public DataType getComponentDataType() {
        return componentDataType;
    }

    public void setComponentDataType(DataType componentDataType) {
        this.componentDataType = componentDataType;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RuleCondition other = (RuleCondition) o;

        return Objects.equal(this.values, other.values) && Objects
                .equal(this.operator, other.operator) && Objects
                .equal(this.type, other.type) && Objects
                .equal(this.componentId, other.componentId) && Objects
                .equal(this.componentDataType, other.componentDataType) && Objects
                .equal(this.timeLimit, other.timeLimit) && Objects
                .equal(this.ruleId, other.ruleId) && Objects
                .equal(this.minimalObservationCountInTimeWindow, other.minimalObservationCountInTimeWindow);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values, operator, type, componentDataType, componentId, timeLimit, ruleId, minimalObservationCountInTimeWindow);
    }
}
