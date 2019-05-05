package org.oisp.rules;

import org.oisp.collection.Observation;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.subCollections.NormalizedStatisticsValues;
import org.oisp.rules.conditions.BaseConditionChecker;
import org.oisp.rules.conditions.ConditionChecker;
import org.oisp.rules.conditions.ConditionFunctionChecker;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticsRuleConditionChecker extends BaseConditionChecker implements ConditionChecker {
    private double average;
    private double standardDeviation;
    private List<String> conditionValues;
    private final NormalizedStatisticsValues values;
    //private Observation observation;
    //private static final Logger logger = LogHelper.getLogger(StatisticsConditionChecker.class);

    public StatisticsRuleConditionChecker(RuleCondition ruleCondition, NormalizedStatisticsValues values) {
        super(ruleCondition);
        this.values = values;
    }

    @Override
    public boolean isConditionFulfilled(Observation observation) {
        //this.observation = observation;
        try {
            if (!hasRequiredObservationCount()) {
                return false;
            }
            calculateStatistics();
            buildConditionValues();
            ConditionFunctionChecker conditionFunctionChecker = createConditionChecker();
            //logger.debug("Avg - {}, Std - {}, Values - {}", average, standardDeviation, Arrays.toString(conditionValues.toArray()));
            return conditionFunctionChecker.isConditionFulfilled(observation.getValue());
        } catch (IOException e) {
            //logger.error("Unable to verify statistics condition for componentId - {}", observation.getCid(), e);
            return false;
        }
    }

    private boolean hasRequiredObservationCount() throws IOException {
        double count = values.getNumSamples();
        return Double.compare(count, getRuleCondition().getMinimalObservationCountInTimeWindow()) >= 0;
    }

    private void calculateStatistics() throws IOException {
        average = values.getAverage();
        standardDeviation = values.getStdDev();
    }

    private void buildConditionValues() {
        conditionValues = getRuleCondition().getValues().stream()
                .map(value -> calculateValue(value))
                .collect(Collectors.toList());
    }

    private ConditionFunctionChecker createConditionChecker() {
        return new ConditionFunctionChecker(getRuleCondition().getOperator(), conditionValues, getRuleCondition().getComponentDataType());
    }

    private String calculateValue(String value) {
        return calculateStatisticsConditionValue(Double.valueOf(value), average, standardDeviation);
    }

    public static String calculateStatisticsConditionValue(double value, double average, double standardDeviation) {
        return String.valueOf(value * standardDeviation + average);
    }
}
