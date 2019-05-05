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

package org.oisp.conf;


import com.google.gson.Gson;

@SuppressWarnings({"checkstyle:methodname", "checkstyle:parametername", "checkstyle:membername", "PMD.TooManyFields" })
public class ExternalConfig {
    private String zookeeper_hbase_quorum;
    private String hbase_table_prefix;
    private String token;
    private String dashboard_url;
    private Boolean dashboard_strict_ssl;
    private String kafka_servers;
    private String kafka_zookeeper_quorum;
    private String kafka_observations_topic;
    private String kafka_rule_engine_topic;
    private String kafka_heartbeat_topic;
    private int kafka_heartbeat_interval;
    private String application_name;
    private String hadoop_security_authentication;
    private String krb_kdc;
    private String krb_password;
    private String krb_user;
    private String krb_realm;
    private String krb_master_principal;
    private String krb_regionserver_principal;
    private String backendType;


    public String getBackendType() {
        return backendType;
    }

    public void setBackendType(String backendType) {
        this.backendType = backendType;
    }

    public String getKafka_observations_topic() {
        return kafka_observations_topic;
    }

    public void setKafka_observations_topic(String kafka_topic) {
        this.kafka_observations_topic = kafka_topic;
    }

    public String getKafka_rule_engine_topic() {
        return kafka_rule_engine_topic;
    }

    public void setKafka_rule_engine_topic(String kafka_topic) {
        this.kafka_rule_engine_topic = kafka_topic;
    }

    public String getKafka_heartbeat_topic() {
        return kafka_heartbeat_topic;
    }

    public void setKafka_heartbeat_topic(String kafka_topic) {
        this.kafka_heartbeat_topic = kafka_topic;
    }

    public int getKafka_heartbeat_interval() {
        return kafka_heartbeat_interval;
    }

    public void setKafka_heartbeat_interval(int interval) {
        this.kafka_heartbeat_interval = interval;
    }

    public String getApplication_name() {
        return application_name;
    }

    public void setApplication_name(String application_name) {
        this.application_name = application_name;
    }

    public String getZookeeper_hbase_quorum() {
        return zookeeper_hbase_quorum;
    }

    public void setZookeeper_hbase_quorum(String zookeeper_hbase_quorum) {
        this.zookeeper_hbase_quorum = zookeeper_hbase_quorum;
    }

    public String getHbase_table_prefix() {
        return hbase_table_prefix;
    }

    public void setHbase_table_prefix(String hbase_table_prefix) {
        this.hbase_table_prefix = hbase_table_prefix;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getDashboard_url() {
        return dashboard_url;
    }

    public void setDashboard_url(String dashboard_url) {
        this.dashboard_url = dashboard_url;
    }

    public String getKafka_servers() {
        return kafka_servers;
    }

    public void setKafka_servers(String kafka_servers) {
        this.kafka_servers = kafka_servers;
    }

    public String getKafka_zookeeper_quorum() {
        return kafka_zookeeper_quorum;
    }

    public void setKafka_zookeeper_quorum(String kafka_zookeeper_quorum) {
        this.kafka_zookeeper_quorum = kafka_zookeeper_quorum;
    }

    public String getKrb_kdc() {
        return krb_kdc;
    }

    public void setKrb_kdc(String krb_kdc) {
        this.krb_kdc = krb_kdc;
    }

    public String getKrb_password() {
        return krb_password;
    }

    public void setKrb_password(String krb_password) {
        this.krb_password = krb_password;
    }

    public String getKrb_user() {
        return krb_user;
    }

    public void setKrb_user(String krb_user) {
        this.krb_user = krb_user;
    }

    public String getKrb_realm() {
        return krb_realm;
    }

    public void setKrb_realm(String krb_realm) {
        this.krb_realm = krb_realm;
    }

    public String getKrb_master_principal() {
        return krb_master_principal;
    }

    public void setKrb_master_principal(String krb_master_principal) {
        this.krb_master_principal = krb_master_principal;
    }

    public String getKrb_regionserver_principal() {
        return krb_regionserver_principal;
    }

    public void setKrb_regionserver_principal(String krb_regionserver_principal) {
        this.krb_regionserver_principal = krb_regionserver_principal;
    }

    public String getHadoop_security_authentication() {
        return hadoop_security_authentication;
    }

    public void setHadoop_security_authentication(String hadoop_security_authentication) {
        this.hadoop_security_authentication = hadoop_security_authentication;
    }

    public Boolean getDashboard_strict_ssl() {
        return dashboard_strict_ssl;
    }

    public void setDashboard_strict_ssl(Boolean dashboard_strict_ssl) {
        this.dashboard_strict_ssl = dashboard_strict_ssl;
    }

    @Override
    public String toString() {
        String sep = ", ";
        StringBuilder builder = new StringBuilder()
                .append("kafka_observations_topic: ").append(getKafka_observations_topic()).append(sep)
                .append("kafka_rule_engine_topic: ").append(getKafka_rule_engine_topic()).append(sep)
                .append("kafka_heartbeat_topic: ").append(getKafka_heartbeat_topic()).append(sep)
                .append("kafka_heartbeat_interval: ").append(getKafka_heartbeat_interval()).append(sep)
                .append("application_name: ").append(getApplication_name()).append(sep)
                .append("zookeeper_hbase_quorum: ").append(getZookeeper_hbase_quorum()).append(sep)
                .append("hbase_table_prefix: ").append(getHbase_table_prefix()).append(sep)
                .append("token: ").append(getToken()).append(sep)
                .append("dashboard_url: ").append(getDashboard_url()).append(sep)
                .append("dashboard_strict_ssl: ").append(getDashboard_strict_ssl()).append(sep)
                .append("kafka_servers: ").append(getKafka_servers()).append(sep)
                .append("kafka_zookeeper_quorum: ").append(getKafka_zookeeper_quorum())
                .append("krb user: ").append(krb_user)
                .append("krb realm: ").append(krb_realm)
                .append("krb kdc: ").append(krb_kdc)
                .append("krb master principal: ").append(krb_master_principal)
                .append("krb region server principal: ").append(krb_regionserver_principal);

        return builder.toString();
    }

    public static ExternalConfig getConfigFromString(String args) {
        Gson g = new Gson();
        return g.fromJson(args, ExternalConfig.class);
    }

    public Config getConfig() {


        Config config = new Config()
                .put(Config.getHbase().ZOOKEEPER_QUORUM, getZookeeper_hbase_quorum())
                .put(Config.getHbase().TABLE_PREFIX, getHbase_table_prefix())
                .put(Config.DASHBOARD_TOKEN_PROPERTY, getToken())
                .put(Config.DASHBOARD_URL_PROPERTY, getDashboard_url())
                .put(Config.DASHBOARD_STRICT_SSL_VERIFICATION, getDashboard_strict_ssl())
                .put(Config.KAFKA_TOPIC_OBSERVATION_PROPERTY, getKafka_observations_topic())
                .put(Config.KAFKA_TOPIC_RULES_UPDATE_PROPERTY, getKafka_rule_engine_topic())
                .put(Config.KAFKA_TOPIC_HEARTBEAT_PROPERTY, getKafka_heartbeat_topic())
                .put(Config.KAFKA_HEARTBEAT_INTERVAL_PROPERTY, getKafka_heartbeat_interval())
                .put(Config.KAFKA_URI_PROPERTY, getKafka_servers())
                .put(Config.KAFKA_ZOOKEEPER_PROPERTY, getKafka_zookeeper_quorum())
                .put(Config.getHbase().AUTHENTICATION_METHOD, getHadoop_security_authentication())
                .put(Config.getKbr().KRB_KDC, getKrb_kdc())
                .put(Config.getKbr().KRB_PASS, getKrb_password())
                .put(Config.getKbr().KRB_REALM, getKrb_realm())
                .put(Config.getKbr().KRB_USER, getKrb_user())
                .put(Config.getKbr().KRB_MASTER_PRINCIPAL, getKrb_master_principal())
                .put(Config.getKbr().KRB_REGIONSERVER_PRINCIPAL, getKrb_regionserver_principal());

        return config;
    }
}
