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

package org.oisp.apiclients.alerts;

import org.oisp.apiclients.ApiClientHelper;
import org.oisp.apiclients.ApiFatalException;
import org.oisp.apiclients.ApiNotAuthorizedException;
import org.oisp.apiclients.ApiNotFatalException;
import org.oisp.apiclients.CustomRestTemplate;
import org.oisp.apiclients.DashboardConfig;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RulesWithObservation;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class DashboardAlertsApi implements AlertsApi, Serializable {

    private final String url;
    private String token;
    private DashboardConfig dashboardConfig;
    private static final String PATH = "/v1/api/alerts";
    private List<RulesWithObservation> rulesWithObservations;

    private transient RestTemplate template;

    public DashboardAlertsApi(DashboardConfigProvider dashboardConfig) {
        this(dashboardConfig, CustomRestTemplate.build().getRestTemplate());
    }

    public DashboardAlertsApi(DashboardConfigProvider dashboardConfig, RestTemplate restTemplate) {
        this.dashboardConfig = dashboardConfig;
        token = this.dashboardConfig.getToken();
        url = this.dashboardConfig.getUrl() + PATH;
        template = restTemplate;
    }

    private String getToken() {
        return token;
    }

    @Override
    public void pushAlert(List<RulesWithObservation> rulesWithObservation) throws ApiFatalException, ApiNotAuthorizedException, ApiNotFatalException {
        this.rulesWithObservations = rulesWithObservation;

        HttpHeaders headers = ApiClientHelper.getHttpHeaders(getToken());
        HttpEntity req = new HttpEntity<>(createAlertBody(), headers);
        try {
            ResponseEntity<String> resp = template.exchange(url, HttpMethod.POST, req, String.class);
            if (resp.getStatusCode() == HttpStatus.FORBIDDEN || resp.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                throw new ApiNotAuthorizedException("Could not get token.");
            }
            if (resp.getStatusCode() != HttpStatus.OK) {
                throw new ApiFatalException("Invalid response - " + resp.getStatusCode());
            }
        } catch (HttpClientErrorException e) {
            if (e.getRawStatusCode() == dashboardConfig.RULENOTACTIVESTATUSCODE) {
                throw new ApiNotFatalException("Rule no longer active - sync problem, no need to repeat.");
            }
            throw new ApiFatalException("General HTTPClient exception: ", e);
        } catch (RestClientException e) {
            throw new ApiFatalException("General REST client exception: ", e);
        }
    }

    private AlertRequest createAlertBody() {
        AlertRequest body = new AlertRequest();
        body.setMsgType(AlertRequest.MSG_TYPE);

        List<Alert> alerts = new ArrayList<>();

        rulesWithObservations.forEach(rules -> {
            rules.getRules().forEach(rule -> {
                alerts.add(createAlertForRule(rule, rules.getObservation()));
            });
        });

        body.setData(alerts);

        return body;
    }

    private Alert createAlertForRule(Rule rule, Observation observation) {
        Alert alert = new Alert();
        alert.setAccountId(rule.getAccountId());
        alert.setRuleId(rule.getId());
        alert.setTimestamp(System.currentTimeMillis());
        alert.setConditions(createAlertCondition(observation));
        alert.setSystemOn(observation.getSystemOn());
        return alert;
    }

    private List<Condition> createAlertCondition(Observation observation) {
        List<Condition> alertConditions = new ArrayList<>();
        alertConditions.add(new Condition(observation));
        return alertConditions;
    }

    @Override
    public String refreshToken() throws ApiFatalException, ApiNotAuthorizedException {
        token = dashboardConfig.refreshToken();
        return token;
    }

    private void readObject(ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
        template = CustomRestTemplate.build().getRestTemplate();
    }
}
