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

package org.oisp.data;

import org.oisp.conf.KerberosProperties;
import org.oisp.conf.HbaseProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

final class HbaseConnManager {

    private final Configuration hbaseConfiguration;
    //private static final Logger logger = LogHelper.getLogger(HbaseConnManager.class);

    private HbaseConnManager(KerberosProperties kerberosProperties, String zkQuorum) {
        this(createHbaseConfiguration(zkQuorum, kerberosProperties));
    }

    private HbaseConnManager(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
    }

    public static HbaseConnManager newInstance(KerberosProperties kerberosProperties, String zkQuorum) {
        return new HbaseConnManager(kerberosProperties, zkQuorum);
    }

    public Connection create() throws IOException {
        return ConnectionFactory.createConnection(hbaseConfiguration);
    }

    private static Configuration createHbaseConfiguration(String zkQuorum, KerberosProperties kerberosProperties) {
        Configuration hbaseConfig = HBaseConfiguration.create();

        if (kerberosProperties.isEnabled()) {
            hbaseConfig.set(HbaseProperties.AUTHENTICATION_METHOD, HbaseProperties.KERBEROS_AUTHENTICATION);
            hbaseConfig.set(HbaseProperties.HBASE_AUTHENTICATION_METHOD, HbaseProperties.KERBEROS_AUTHENTICATION);
            hbaseConfig.set(KerberosProperties.KRB_MASTER_PRINCIPAL, kerberosProperties.getMasterPrincipal());
            hbaseConfig.set(KerberosProperties.KRB_REGIONSERVER_PRINCIPAL, kerberosProperties.getRegionServerPrinicipal());
        }

        hbaseConfig.set(HbaseProperties.ZOOKEEPER_QUORUM, zkQuorum);

        return hbaseConfig;
    }
}
