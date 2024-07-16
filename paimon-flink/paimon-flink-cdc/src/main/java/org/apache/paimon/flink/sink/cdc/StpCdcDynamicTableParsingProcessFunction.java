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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different side outputs according to table name. This process
 * function will capture newly added tables when syncing entire database and in cases where the
 * newly added tables are including by attesting table filters.
 *
 * <p>This {@link ProcessFunction} can handle records for different tables at the same time.
 */
public class StpCdcDynamicTableParsingProcessFunction extends ProcessFunction<StpCdcRecord, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(StpCdcDynamicTableParsingProcessFunction.class);

    public static final OutputTag<CdcMultiplexRecord> DYNAMIC_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-dynamic-table(FixedBucket)",
                    TypeInformation.of(CdcMultiplexRecord.class));

    public static final OutputTag<CdcMultiplexRecord> DYNAMIC_BUCKET_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-dynamic-table(DynamicBucket)",
                    TypeInformation.of(CdcMultiplexRecord.class));

    public static final OutputTag<CdcMultiplexRecord> UNAWARE_BUCKET_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-dynamic-table(UnawareBucket)",
                    TypeInformation.of(CdcMultiplexRecord.class));

    public static final OutputTag<Tuple2<Identifier, List<DataField>>>
            DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-dynamic-table-schema-change",
                    TypeInformation.of(
                            new TypeHint<Tuple2<Identifier, List<DataField>>>() {
                            }));

    private final EventParser.Factory<StpCdcRecord> parserFactory;
    private final Catalog.Loader catalogLoader;
    private transient Catalog catalog;
    private final Set<BucketMode> excludeBucketModes;

    private transient EventParser<StpCdcRecord> parser;
    private transient Map<Identifier, FileStoreTable> tableMap;

    public StpCdcDynamicTableParsingProcessFunction(
            Catalog.Loader catalogLoader,
            EventParser.Factory<StpCdcRecord> parserFactory,
            Set<BucketMode> excludeBucketModes) {
        this.catalogLoader = catalogLoader;
        this.parserFactory = parserFactory;
        this.excludeBucketModes = excludeBucketModes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.parser = parserFactory.create();
        this.tableMap = new HashMap<>();
        this.catalog = catalogLoader.load();
    }

    @Override
    public void processElement(StpCdcRecord raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);
        String tableName = parser.parseTableName();
        Identifier identifier = Identifier.fromString(tableName);
        FileStoreTable table = TableSelector.getTable(this.tableMap, identifier, raw, catalog);
        if (!tableMap.containsKey(identifier)
                && table == null) {
            throw new IllegalArgumentException(
                    "Paimon table not exists:" + identifier.getEscapedFullName());
        }

        List<DataField> schemaChange = parser.parseSchemaChange();
        if (!schemaChange.isEmpty()) {
            context.output(DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG, Tuple2.of(identifier, schemaChange));
        }

        BucketMode bucketMode = table.bucketMode();
        if (excludeBucketModes.contains(bucketMode)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unaccepted bucket mode %s of table %s, exclude bucket modes: %s",
                            bucketMode, identifier.getFullName(), excludeBucketModes));
        }

        OutputTag<CdcMultiplexRecord> outputTag;
        switch (bucketMode) {
            case HASH_DYNAMIC:
                outputTag = DYNAMIC_BUCKET_OUTPUT_TAG;
                break;
            case HASH_FIXED:
                outputTag = DYNAMIC_OUTPUT_TAG;
                break;
            case BUCKET_UNAWARE:
                outputTag = UNAWARE_BUCKET_OUTPUT_TAG;
                break;
            default:
                throw new UnsupportedOperationException("unsupported bucket mode: " + bucketMode);
        }
        OutputTag<CdcMultiplexRecord> finalOutputTag = outputTag;
        parser.parseRecords()
                .forEach(
                        record ->
                                context.output(
                                        finalOutputTag,
                                        wrapRecord(
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName(),
                                                record)));
    }


    private CdcMultiplexRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return CdcMultiplexRecord.fromCdcRecord(databaseName, tableName, record);
    }
}
