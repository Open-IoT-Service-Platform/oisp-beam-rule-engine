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

package org.oisp.apiclients.auth;

import org.oisp.apiclients.CustomRestTemplate;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;


public class DashboardAuthApi implements AuthApi, Serializable {

    private DashboardConfigProvider config;
    private static final String PATH = "/v1/api/auth";
    private static final String POSTTOKEN = "/token";

    private transient RestTemplate template;

    public DashboardAuthApi(DashboardConfigProvider dashboardConfig) {
        this(dashboardConfig, CustomRestTemplate.build().getRestTemplate());
    }

    public DashboardAuthApi(DashboardConfigProvider dashboardConfig, RestTemplate restTemplate) {
        template = restTemplate;
        config = dashboardConfig;
    }

    @Override
    public String getToken(String username, String password) throws InvalidDashboardResponseException {

        //HttpHeaders headers = ApiClientHelper.getHttpHeaders(config.getToken());
        HttpEntity req = new HttpEntity<>(createGetTokenBody(username, password), null);
        ResponseEntity<AuthResponse> resp;

        try {
            resp = template.exchange(config.getUrl() + PATH + POSTTOKEN, HttpMethod.POST, req, AuthResponse.class);
            if (resp.getStatusCode() != HttpStatus.OK) {
                throw new InvalidDashboardResponseException("Invalid response - " + resp.getStatusCode());
            }
        } catch (RestClientException e) {
            throw new InvalidDashboardResponseException("Unknown dashboard response error.", e);
        }
        return resp.getBody().getToken();
    }

    private AuthRequest createGetTokenBody(String username, String password) {
        AuthRequest body = new AuthRequest();
        body.setUsername(username);
        body.setPassword(password);

        return body;
    }


    private void readObject(ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
        template = CustomRestTemplate.build().getRestTemplate();
    }
}
