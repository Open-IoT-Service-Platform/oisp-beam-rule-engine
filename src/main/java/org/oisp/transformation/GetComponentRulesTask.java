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


import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.values.PCollectionView;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.rules.DashboardRulesApi;
import org.oisp.apiclients.rules.RulesApi;
import org.oisp.apiclients.rules.model.ComponentRulesResponse;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RulesWithObservation;
import org.oisp.conf.Config;
import org.oisp.parsers.RuleParser;
import org.oisp.utils.LogHelper;


import org.slf4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class GetComponentRulesTask extends DoFn<List<Observation>, List<RulesWithObservation>> {

    private List<Observation> observations;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);
    private PCollectionView<List<Long>> sideInput;
    private RulesApi rulesApi;
    private Map<String, List<Rule>> componentsRules;
    private Long componentRuleversion;

    @Setup
    public void setup() {
        updateComponentRules();
        componentRuleversion = 0L;

    }
    public GetComponentRulesTask(Config userConfig, PCollectionView<List<Long>> sideInput) {
        //this(new RulesHbaseRepository(userConfig));
        rulesApi = new DashboardRulesApi(new DashboardConfigProvider(userConfig));
        this.sideInput = sideInput;
    }

    private void updateComponentRules() {
        try {
            componentsRules = getComponentsRules();
        } catch (InvalidDashboardResponseException e) {
            LOG.error("Error during updating rules - ", e);
        }
    }
    /*public GetComponentRulesTask(RulesRepository rulesRepository) {
        this.rulesRepository = rulesRepository;
    }*/

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            observations = c.element();
            //check componentRuleversion/sequence number to trigger rules update if needed
            //Long newComponentRuleVersion = c.sideInput(sideInput).get("ver");
            Long newComponentRuleVersion = c.sideInput(sideInput).get(0);
            System.out.println("Marcel293: " + newComponentRuleVersion + " " + componentRuleversion);
            if (newComponentRuleVersion != componentRuleversion) {
                updateComponentRules();
                componentRuleversion = newComponentRuleVersion;
            }
            c.output(getActiveObservations());
        } catch (IOException e) {
            LOG.error("Error during searching rules for observations - ", e);
        }
    }

    @Teardown
    public void teardown() {
        System.out.println("Teardown of Transform.");
    }
    private List<RulesWithObservation> getActiveObservations() throws IOException {
        List<RulesWithObservation> rulesWithObservations = getRulesWithObservation(observations);

        List<RulesWithObservation> observationsWithActiveRules = rulesWithObservations.stream()
                .filter(r -> hasObservationRules(r))
                .collect(Collectors.toList());

        return observationsWithActiveRules;
    }

    private List<RulesWithObservation> getRulesWithObservation(List<Observation> observations) throws IOException {
        return observations.stream()
                .map(observation -> new RulesWithObservation(observation, componentsRules.get(observation.getCid())))
                .collect(Collectors.toList());
    }

    private boolean hasObservationRules(RulesWithObservation rulesWithObservation) {
        return rulesWithObservation.getRules() != null && rulesWithObservation.getRules().size() > 0;
    }

    private Map<String, List<Rule>> getComponentsRules() throws InvalidDashboardResponseException {
        List<ComponentRulesResponse> componentsRules = rulesApi.getActiveComponentsRules(true);
        RuleParser ruleParser = new RuleParser(componentsRules);
        Map<String, List<Rule>> result = ruleParser.getComponentRules();
        return result;
    }
}
