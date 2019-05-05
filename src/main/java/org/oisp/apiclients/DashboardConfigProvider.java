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

import org.oisp.conf.Config;
import java.io.Serializable;

public class DashboardConfigProvider implements DashboardConfig, Serializable {

    private final String token;
    private final String url;
    private final boolean strictSSL;

    public DashboardConfigProvider(Config userConfig) {
        Object token = userConfig.get(Config.DASHBOARD_TOKEN_PROPERTY);
        Object url = userConfig.get(Config.DASHBOARD_URL_PROPERTY);

        if (token != null) {
            this.token = token.toString();
        } else {
            this.token = null;
        }

        if (url != null) {
            this.url = url.toString();
        } else {
            this.url = null;
        }

        this.strictSSL = parseStrictSSLOption(userConfig);
    }

    private Boolean parseStrictSSLOption(Config userConfig) {
        String strictSSL = userConfig.get(Config.DASHBOARD_STRICT_SSL_VERIFICATION).toString();
        //By default strictSSL option should be enabled
        if (strictSSL == null) {
            return true;
        }
        return Boolean.valueOf(strictSSL);
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public boolean isStrictSSL() {
        return strictSSL;
    }

}
