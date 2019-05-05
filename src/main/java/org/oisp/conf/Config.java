package org.oisp.conf;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Config implements Serializable {

    public static final String KAFKA_TOPIC_RULES_UPDATE_PROPERTY = "KAFKA_RULES_UPDATE_TOPIC";
    public static final String KAFKA_ZOOKEEPER_PROPERTY = "KAFKA_URI_ZOOKEEPER";
    public static final String KAFKA_URI_PROPERTY = "KAFKA_URI";
    public static final String KAFKA_TOPIC_HEARTBEAT_PROPERTY = "KAFKA_HEARTBEAT_TOPIC";
    public static final String KAFKA_HEARTBEAT_INTERVAL_PROPERTY = "KAFKA_HEARTBEAT_INTERVAL";
    public static final String KAFKA_TOPIC_OBSERVATION_PROPERTY = "KAFKA_OBSERVATIONS_TOPIC";
    public static final String DASHBOARD_TOKEN_PROPERTY = "DASHBOARD_TOKEN";
    public static final String DASHBOARD_URL_PROPERTY = "DASHBOARD_URL";
    public static final String DASHBOARD_STRICT_SSL_VERIFICATION = "DASHBOARD_STRICT_SSL";


    private static HbaseProperties hbase;
    private static KerberosProperties kbr;

    private Map<String, Object> conf;
    Config() {
        conf = new HashMap<String, Object>();
    }

    public Object get(String key) {
        return conf.get(key);
    }

    public Config put(String key, Object value) {
        conf.put(key, value);
        return this;
    }
    public Map<String, Object> getHash() {
        return conf;
    }

    public Config put(Map<String, Object> config) {
        conf = config;
        return this;
    }
    public String[] getStringArray(String key) {
        String[] result;
        try {
            result = ((List<String>) conf.get(key)).toArray(new String[0]);
        } catch (NullPointerException exception) {
            result = null;
        }
        return result;
    }

    public static HbaseProperties getHbase() {
        return hbase;
    }

    public static void setHbase(HbaseProperties hbase) {
        Config.hbase = hbase;
    }

    public static KerberosProperties getKbr() {
        return kbr;
    }

    public static void setKbr(KerberosProperties kbr) {
        Config.kbr = kbr;
    }
}
