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

import org.apache.beam.sdk.values.KV;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.ApiFatalException;
import org.oisp.apiclients.ApiNotAuthorizedException;
import org.oisp.apiclients.rules.DashboardRulesApi;
import org.oisp.apiclients.rules.RulesApi;
import org.oisp.apiclients.rules.model.ComponentRulesResponse;
import org.oisp.parsers.RuleParser;
import org.oisp.collection.Rule;
import org.oisp.conf.Config;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;
import java.util.Map;

@SuppressWarnings({"checkstyle:illegalcatch", "PMD.AvoidCatchingGenericException"})
public class DownloadRulesTask  extends DoFn<KV<String, String>, KV<String, Map<String, List<Rule>>>> {

    private final RulesApi rulesApi;
    private Map<String, List<Rule>> componentsRules;
    public DownloadRulesTask(Config userConfig) throws ApiFatalException, ApiNotAuthorizedException {
        this(new DashboardRulesApi(new DashboardConfigProvider(userConfig)));
    }

    public DownloadRulesTask(RulesApi rulesApi) {
        this.rulesApi = rulesApi;
    }


    @ProcessElement
    public void processElement(ProcessContext c) throws ApiFatalException {
        try {
            componentsRules = getComponentsRules();
        } catch (ApiNotAuthorizedException e) {
            try {
                rulesApi.refreshToken();
                componentsRules = getComponentsRules();
            } catch (ApiFatalException | ApiNotAuthorizedException e2) { //NotAuthorized exception a 2nd time will cancel the pipeline
                throw new ApiFatalException("Could not download rules: ", e2);
            }
        }
        c.output(KV.of("global", componentsRules));
    }

    private Map<String, List<Rule>> getComponentsRules() throws ApiFatalException, ApiNotAuthorizedException {
        List<ComponentRulesResponse> componentsRules = rulesApi.getActiveComponentsRules(false);
        RuleParser ruleParser = new RuleParser(componentsRules);
        Map<String, List<Rule>> result = ruleParser.getComponentRules();
        return result;
    }

}
