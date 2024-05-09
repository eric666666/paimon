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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link EventParser} for {@link RichCdcMultiplexRecord}.
 */
public class StpRichCdcMultiplexRecordEventParser implements EventParser<StpRichCdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(StpRichCdcMultiplexRecordEventParser.class);

    @Nullable
    private final StpNewTableSchemaBuilder schemaBuilder;
    private final Map<String, RichEventParser> parsers = new HashMap<>();
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();
    private final Set<String> createdTables = new HashSet<>();

    private StpRichCdcMultiplexRecord record;
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;
    private RichEventParser currentParser;

    //public StpRichCdcMultiplexRecordEventParser(boolean caseSensitive) {
    //    this(null, null, null, new TableNameConverter(caseSensitive));
    //}
    //
    public StpRichCdcMultiplexRecordEventParser(
            @Nullable StpNewTableSchemaBuilder schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override
    public void setRawEvent(StpRichCdcMultiplexRecord record) {
        this.record = record;
        this.currentTable = record.tableName();
        this.shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        if (shouldSynchronizeCurrentTable) {
            this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
            this.currentParser.setRawEvent(record.toRichCdcRecord());
        }
    }

    @Override
    public String parseTableName() {
        return record.databaseName() + "." + record.tableName();
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return shouldSynchronizeCurrentTable
                ? currentParser.parseSchemaChange()
                : Collections.emptyList();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        return shouldSynchronizeCurrentTable
                ? currentParser.parseRecords()
                : Collections.emptyList();
    }

    @Override
    public Optional<Schema> parseNewTable() {
        return schemaBuilder.build(record);
    }

    private boolean shouldSynchronizeCurrentTable() {
        // In case the record is incomplete, we let the null value pass validation
        // and handle the null value when we really need it
        if (currentTable == null) {
            return true;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }

    //private boolean shouldCreateCurrentTable() {
    //    return shouldSynchronizeCurrentTable
    //            && !record.fields().isEmpty()
    //            && createdTables.add(parseTableName());
    //}
}
