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

package org.oisp.apiclients;

import org.oisp.apiclients.auth.DashboardAuthApi;
import org.oisp.conf.Config;
import java.io.Serializable;

public class DashboardConfigProvider implements DashboardConfig, Serializable {

    private String token;
    private  String url;
    private  String username;
    private  String password;

    public DashboardConfigProvider(Config userConfig) throws InvalidDashboardResponseException {
        Object token = userConfig.get(Config.DASHBOARD_TOKEN_PROPERTY);
        Object url = userConfig.get(Config.DASHBOARD_URL_PROPERTY);
        Object username = userConfig.get(Config.DASHBOARD_USERNAME_PROPERTY);
        Object password = userConfig.get(Config.DASHBOARD_PASSWORD_PROPERTY);

        if (url != null) {
            this.url = url.toString();
        } else {
            throw new NullPointerException("Dashboard url is not defined");
        }

        if (token != null) {
            this.token = token.toString();
        } else {

            if (username != null) {
                this.username = username.toString();
            } else {
                throw new NullPointerException("No token AND no username is defined.");
            }

            if (password != null) {
                this.password = password.toString();
            } else {
                throw new NullPointerException("No token AND no password is defined.");
            }

            DashboardAuthApi dashboardAuthApi = new DashboardAuthApi(this);
            this.token = dashboardAuthApi.getToken(this.username, this.password);
        }
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String getToken() {
        return token;
    }
}
