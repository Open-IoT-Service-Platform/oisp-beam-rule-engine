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

//import org.apache.http.client.HttpClient;
//import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
//import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import javax.net.ssl.SSLContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;


public final class CustomRestTemplate {


    private static final Logger logger = LogHelper.getLogger(CustomRestTemplate.class);
    private final RestTemplate template;

    private CustomRestTemplate(DashboardConfig dashboardConfig) {
        if (!dashboardConfig.isStrictSSL()) {
            template = new RestTemplate(createHttpRequestFactory());
        } else {
            template = new RestTemplate();
        }
    }

    public static CustomRestTemplate build(DashboardConfig dashboardConfig) {
        return new CustomRestTemplate(dashboardConfig);
    }

    public RestTemplate getRestTemplate() {
        return template;
    }

    private ClientHttpRequestFactory createHttpRequestFactory() {
        try {
            TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] certificate, String authType) {
                    return true;
                }
            };
            SSLContext sslContext = new SSLContextBuilder()
                    .loadTrustMaterial(null, acceptingTrustStrategy)
                    .build();
            SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);
            CloseableHttpClient httpClient = HttpClients.custom()
                    .setSSLSocketFactory(csf)
                    .build();
            HttpComponentsClientHttpRequestFactory requestFactory =
                    new HttpComponentsClientHttpRequestFactory();
            requestFactory = new HttpComponentsClientHttpRequestFactory();
            requestFactory.setHttpClient(httpClient);
            return requestFactory;
        } catch (GeneralSecurityException e) {
            logger.error("Error during disabling strict ssl certificate verification", e);
        }
        return null;
    }
}
