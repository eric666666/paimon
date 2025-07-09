/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * StpCdcRecord. Compared to {@link CdcMultiplexRecord}, this contains schema information. Compared
 * to {@link RichCdcMultiplexRecord} remove primary key.
 */
public class StpCdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private String databaseName;
    private String tableName;
    /** to update data field. */
    private  CdcSchema cdcSchema;
    private CdcRecord cdcRecord;
    private Table table;

    public StpCdcRecord(
            String databaseName, String tableName, CdcSchema cdcSchema, CdcRecord cdcRecord) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.cdcSchema = cdcSchema == null ? CdcSchema.newBuilder().build() : cdcSchema;
        this.cdcRecord = cdcRecord;
    }

    public StpCdcRecord(String databaseName, String tableName, CdcRecord cdcRecord) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.cdcSchema = CdcSchema.newBuilder().build();
        this.cdcRecord = cdcRecord;
    }

    @Nullable
    public String databaseName() {
        return databaseName;
    }

    @Nullable
    public String tableName() {
        return tableName;
    }

    public CdcSchema cdcSchema() {
        return cdcSchema;
    }

    public Table getTable() {
        return table;
    }

    public RichCdcRecord toRichCdcRecord() {
        return new RichCdcRecord(cdcRecord, cdcSchema);
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setCdcSchema(CdcSchema cdcSchema) {
        this.cdcSchema = cdcSchema;
    }

    public void setCdcRecord(CdcRecord cdcRecord) {
        this.cdcRecord = cdcRecord;
    }

    public CdcRecord getCdcRecord() {
        return cdcRecord;
    }

    public CdcSchema getCdcSchema() {
        return cdcSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, cdcSchema, cdcRecord);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StpCdcRecord that = (StpCdcRecord) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(cdcSchema, that.cdcSchema)
                && Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public String toString() {
        return "{"
                + "databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + ", cdcSchema="
                + cdcSchema
                + ", cdcRecord="
                + cdcRecord
                + '}';
    }
}
