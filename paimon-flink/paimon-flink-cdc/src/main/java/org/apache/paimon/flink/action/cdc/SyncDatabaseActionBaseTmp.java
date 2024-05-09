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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkBuilderTmp;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.sink.cdc.StpRichCdcMultiplexRecord;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Base {@link Action} for synchronizing into one Paimon database.
 */
public abstract class SyncDatabaseActionBaseTmp extends SynchronizationActionBaseTmp {

    protected boolean mergeShards = true;
    protected MultiTablesSinkMode mode;//= COMBINED;
    //protected String tablePrefix = "";
    //protected String tableSuffix = "";
    //protected String includingTables = ".*";
    //@Nullable protected String excludingTables;
    protected List<FileStoreTable> tables = new ArrayList<>();

    public SyncDatabaseActionBaseTmp(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig,
            SyncJobHandler.SourceType sourceType) {
        super(
                warehouse,
                database,
                catalogConfig,
                cdcSourceConfig,
                new SyncJobHandler(sourceType, cdcSourceConfig, database));
    }

    public SyncDatabaseActionBaseTmp mergeShards(boolean mergeShards) {
        this.mergeShards = mergeShards;
        return this;
    }

    public SyncDatabaseActionBaseTmp withMode(MultiTablesSinkMode mode) {
        this.mode = mode;
        return this;
    }


    @Override
    protected void validateCaseSensitivity() {
        AbstractCatalog.validateCaseInsensitive(caseSensitive, "Database", database);
    }

    @Override
    protected FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse() {
        return syncJobHandler.provideRecordParser(
                caseSensitive, Collections.emptyList(), typeMapping, metadataConverters);
    }

    @Override
    protected EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory() {
        NewTableSchemaBuilder schemaBuilder =
                new NewTableSchemaBuilder(tableConfig, caseSensitive, metadataConverters);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);

        return () ->
                new RichCdcMultiplexRecordEventParser(
                        schemaBuilder, includingPattern, excludingPattern, tableNameConverter);
    }

    @Override
    protected void buildSink(
            DataStream<StpRichCdcMultiplexRecord> input,
            EventParser.Factory<StpRichCdcMultiplexRecord> parserFactory) {
        new FlinkCdcSyncDatabaseSinkBuilderTmp<StpRichCdcMultiplexRecord>()
                .withInput(input)
                .withParserFactory(parserFactory)
                .withCatalogLoader(catalogLoader())
                .withTables(tables)
                .withMode(mode)
                .withTableOptions(tableConfig)
                .build();
    }
}
