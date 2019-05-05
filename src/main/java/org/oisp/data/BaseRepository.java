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

import org.oisp.conf.Config;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.Serializable;
import java.util.concurrent.Semaphore;

import javax.annotation.PreDestroy;
import java.io.IOException;

public abstract class BaseRepository implements Serializable {

    private final String tableName;
    private final String zkQuorum;
    private final Config userConfig;
    private transient Connection hbaseConnection;
    private static Semaphore mutex = new Semaphore(1);

    public BaseRepository(Config userConfig) {
        String tablePrefix = userConfig.get(Config.getHbase().TABLE_PREFIX).toString();
        tableName = buildTableName(tablePrefix);
        zkQuorum = userConfig.get(Config.getHbase().ZOOKEEPER_QUORUM).toString();
        this.userConfig = userConfig;
    }

    public void createTable() throws IOException {
        try (Admin admin = getHbaseAdmin()) {
            mutex.acquire();
            if (!admin.tableExists(getTableName())) {
                HTableDescriptor table = new HTableDescriptor(getTableName());
                for (String family : HbaseValues.TABLES_COLUMN_FAMILIES.get(getTableNameWithoutPrefix())) {
                    table.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(table);
                addCoprocessor(admin, table);
            }
            mutex.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected abstract void addCoprocessor(Admin admin, HTableDescriptor tableDescriptor) throws IOException;

    private String buildTableName(String prefix) {
        return new StringBuilder(prefix).append(getTableNameWithoutPrefix()).toString();
    }

    public TableName getTableName() {
        return TableName.valueOf(tableName);
    }

    public abstract String getTableNameWithoutPrefix();

    private Connection getHbaseConnection() throws IOException {
        if (hbaseConnection == null || hbaseConnection.isClosed()) {
            hbaseConnection =  HbaseConnManager.newInstance(Config.getKbr().fromConfig(userConfig), zkQuorum).create();
        }
        return hbaseConnection;
    }

    @PreDestroy
    protected void closeConnection() throws IOException {
        if (hbaseConnection != null && !hbaseConnection.isClosed()) {
            hbaseConnection.close();
        }
    }

    protected Table getTable() throws IOException {
        return getHbaseConnection().getTable(getTableName());
    }

    protected Admin getHbaseAdmin() throws IOException {
        return getHbaseConnection().getAdmin();
    }

}
