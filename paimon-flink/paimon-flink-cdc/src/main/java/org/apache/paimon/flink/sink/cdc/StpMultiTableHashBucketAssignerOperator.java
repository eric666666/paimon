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
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.SimpleHashBucketAssigner;
import org.apache.paimon.index.StpHashBucketAssigner;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Assign bucket for the input record, output record with bucket.
 */
public class StpMultiTableHashBucketAssignerOperator
        extends AbstractStreamOperator<Tuple2<CdcMultiplexRecord, Integer>>
        implements OneInputStreamOperator<CdcMultiplexRecord, Tuple2<CdcMultiplexRecord, Integer>> {

    private static final long serialVersionUID = 1L;

    private final String initialCommitUser;

    private final Integer numAssigners;
    private final SerializableFunction<TableSchema, PartitionKeyExtractor<CdcMultiplexRecord>>
            extractorFunction;

    private final boolean overwrite;

    private final Map<Identifier, BucketAssigner> assignerHolder = new HashMap<>();
    private final Map<Identifier, FileStoreTable> tables = new HashMap<>();
    private final Map<Identifier, PartitionKeyExtractor<CdcMultiplexRecord>> extractors =
            new HashMap<>();
    private final Catalog.Loader catalogLoader;
    private  transient Catalog catalog;
    private final long indexExpireTimestamp;

    private int numberTasks;
    private int taskId;
    private String commitUser;

    public StpMultiTableHashBucketAssignerOperator(
            String commitUser,
            Catalog.Loader catalogLoader,
            Integer numAssigners,
            SerializableFunction<TableSchema, PartitionKeyExtractor<CdcMultiplexRecord>>
                    extractorFunction,
            boolean overwrite,
            long indexExpireTimestamp) {
        this.initialCommitUser = commitUser;
        this.catalogLoader = catalogLoader;
        this.numAssigners = numAssigners;
        this.extractorFunction = extractorFunction;
        this.overwrite = overwrite;
        this.indexExpireTimestamp = indexExpireTimestamp;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        this.catalog = catalogLoader.load();
        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        this.commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        this.numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.taskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<CdcMultiplexRecord> streamRecord) throws Exception {
        CdcMultiplexRecord value = streamRecord.getValue();
        Identifier identifier = Identifier.create(value.databaseName(), value.tableName());
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        if (!this.assignerHolder.containsKey(identifier)) {
            long targetRowNum = table.coreOptions().dynamicBucketTargetRowNum();
            BucketAssigner assigner =
                    overwrite
                            ? new SimpleHashBucketAssigner(numberTasks, taskId, targetRowNum)
                            : new StpHashBucketAssigner(
                            table.snapshotManager(),
                            commitUser,
                            table.store().newIndexFileHandler(),
                            numberTasks,
                            MathUtils.min(numAssigners, numberTasks),
                            taskId,
                            targetRowNum,
                            indexExpireTimestamp);
            PartitionKeyExtractor<CdcMultiplexRecord> extractor =
                    extractorFunction.apply(table.schema());

            this.assignerHolder.put(identifier, assigner);
            this.extractors.put(identifier, extractor);
        }
        BucketAssigner assigner = this.assignerHolder.get(identifier);
        PartitionKeyExtractor<CdcMultiplexRecord> extractor = this.extractors.get(identifier);
        int bucket =
                assigner.assign(
                        extractor.partition(value), extractor.trimmedPrimaryKey(value).hashCode());
        output.collect(new StreamRecord<>(new Tuple2<>(value, bucket)));
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        for (BucketAssigner assigner : assignerHolder.values()) {
            assigner.prepareCommit(checkpointId);
        }
    }
}
