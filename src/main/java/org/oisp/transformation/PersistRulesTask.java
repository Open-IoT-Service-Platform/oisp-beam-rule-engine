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

package org.oisp.transformation;

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.rules.DashboardRulesApi;
import org.oisp.apiclients.rules.RulesApi;
import org.oisp.collection.Rule;
import org.oisp.conf.Config;

import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.UnusedPrivateField")
public class PersistRulesTask extends DoFn<KV<String, Map<String, List<Rule>>>, Long> {
    @DoFn.StateId("counter")
    private final StateSpec<ValueState<Long>> stateSpec = StateSpecs.value();
    private final RulesApi rulesApi;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    public PersistRulesTask(Config userConfig) {
        this(new DashboardRulesApi(new DashboardConfigProvider(userConfig)));
    }

    public PersistRulesTask(RulesApi rulesApi) {
        this.rulesApi = rulesApi;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @DoFn.StateId("counter") ValueState<Long> state) {
        LOG.info("Persisting dashboard rules...");
        try {
            Map<String, List<Rule>> rules = c.element().getValue();
            Long counter = state.read();
            if (counter == null) {
                counter = 0L;
            }
            if (!isRulesEmpty(rules)) {
                rulesApi.markRulesSynchronized(getRulesIds(rules.values()));
                counter++;
                state.write(counter);
                System.out.println("Marcel245:" + counter);
                c.output(counter);
            }
        } catch (InvalidDashboardResponseException e) {
            LOG.error("Unable to mark persisted rules as synchronized in Dashboard", e);
        }
    }

    private Set<String> getRulesIds(Collection<List<Rule>> ruleCollection) {
        return ruleCollection.stream()
                .flatMap(rules -> rules.stream())
                .map(rule -> rule.getId())
                .collect(Collectors.toSet());
    }

    private boolean isRulesEmpty(Map<String, List<Rule>> rules) {
        return rules == null || rules.isEmpty();
    }
}
