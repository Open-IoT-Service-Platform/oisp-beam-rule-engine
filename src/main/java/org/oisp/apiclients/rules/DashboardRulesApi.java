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

package org.oisp.apiclients.rules;

import org.oisp.apiclients.ApiClientHelper;
import org.oisp.apiclients.ApiFatalException;
import org.oisp.apiclients.ApiNotAuthorizedException;
import org.oisp.apiclients.CustomRestTemplate;
import org.oisp.apiclients.DashboardConfig;
import org.oisp.apiclients.rules.model.ComponentRulesResponse;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.IOException;



public class DashboardRulesApi implements RulesApi, Serializable {

    private final String url;
    private String token;
    private DashboardConfig dashboardConfig;
    private static final String PATH = "/v1/api/";
    private static final String GET_COMPONENTS_RULES_PATH = "components/rules";
    private static final String UPDATE_RULES_PATH = "rules/synchronization_status/Sync";

    private static final String RULE_STATUS_NOT_SYNCHRONIZED = "NotSync";
    private static final String RULE_STATUS_SYNCHRONIZED = "Sync";
    private static final String COULDNOTGETTOKEN = "Could not get token.";

    private transient RestTemplate template;

    public DashboardRulesApi(DashboardConfig dashboardConfig) {
        this(dashboardConfig, CustomRestTemplate.build().getRestTemplate());
    }

    public DashboardRulesApi(DashboardConfig dashboardConfig, RestTemplate restTemplate) {
        this.dashboardConfig = dashboardConfig;
        token = this.dashboardConfig.getToken();
        url = this.dashboardConfig.getUrl() + PATH;
        template = restTemplate;
    }

    @Override
    public List<ComponentRulesResponse> getActiveComponentsRules(Boolean synced) throws ApiFatalException, ApiNotAuthorizedException {
        HttpHeaders headers = ApiClientHelper.getHttpHeaders(getToken());
        HttpEntity req = new HttpEntity<>(createRuleRequest(synced), headers);

        try {
            ParameterizedTypeReference<List<ComponentRulesResponse>> responseType = new ParameterizedTypeReference<List<ComponentRulesResponse>>() {
            };
            ResponseEntity<List<ComponentRulesResponse>> resp = template.exchange(getEndpoint(GET_COMPONENTS_RULES_PATH), HttpMethod.POST, req, responseType);
            if (resp.getStatusCode() == HttpStatus.FORBIDDEN || resp.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                throw new ApiNotAuthorizedException(COULDNOTGETTOKEN);
            }
            if (resp.getStatusCode() != HttpStatus.OK) {
                throw new ApiFatalException("Invalid response on - " + resp.getStatusCode());
            }

            return resp.getBody();
        } catch (RestClientException e) {
            throw new ApiFatalException("Unknown dashboard response error.", e);
        }
    }

    @Override
    public void markRulesSynchronized(Set<String> rulesIds) throws ApiFatalException, ApiNotAuthorizedException {
        HttpHeaders headers = ApiClientHelper.getHttpHeaders(getToken());
        HttpEntity req = new HttpEntity<>(rulesIds, headers);

        try {
            ResponseEntity<Void> resp = template.exchange(getEndpoint(UPDATE_RULES_PATH), HttpMethod.PUT, req, Void.class);
            if (resp.getStatusCode() == HttpStatus.FORBIDDEN || resp.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                throw new ApiNotAuthorizedException(COULDNOTGETTOKEN);
            }
            if (resp.getStatusCode() != HttpStatus.OK) {
                throw new ApiFatalException("Invalid response when updating synchronization status - " + resp.getStatusCode());
            }
        } catch (RestClientException e) {
            throw new ApiFatalException("Unknown dashboard response error when updating synchronization status.", e);
        }
    }

    private RuleRequest createRuleRequest(Boolean synced) {
        String syncStatus = RULE_STATUS_NOT_SYNCHRONIZED;
        if (synced) {
            syncStatus = RULE_STATUS_SYNCHRONIZED;
        }
        List<String> ruleStatus = new ArrayList<String>();
        ruleStatus.add("Active");
        // In the current setup we look only for Active rules
        // This will change once we have Cassandra to manage changes and only handle unsynced rules
        return new RuleRequest(ruleStatus, syncStatus);
    }


    private String getEndpoint(String restMethod) {
        return url + restMethod;
    }

    private String getToken() {
        return token;
    }

    @Override
    public String refreshToken() throws ApiFatalException {
        token = dashboardConfig.refreshToken();
        return token;
    }

    private void readObject(ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
        template = CustomRestTemplate.build().getRestTemplate();
    }
}
