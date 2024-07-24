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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.CommitterOperator;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.flink.sink.RestoreAndFailCommittableStateManager;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.WrappedManifestCommittableSerializer;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.paimon.flink.sink.FlinkSink.assertStreamingConfiguration;
import static org.apache.paimon.flink.sink.FlinkSink.configureGlobalCommitter;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.index.BucketAssigner.computeAssigner;

/**
 * A {@link FlinkSink} which accepts {@link CdcRecord} and waits for a schema change if necessary.
 */
public class StpFlinkCdcMultiDynamicBucketTableSink implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String WRITER_NAME = "StpFlinkCdcMultiDynamicBucketTable MultiplexWriter";
    private static final String GLOBAL_COMMITTER_NAME = "StpFlinkCdcMultiDynamicBucketTable Global Committer";

    private final boolean isOverwrite = false;
    private final Catalog.Loader catalogLoader;
    private final double commitCpuCores;
    @Nullable
    private final MemorySize commitHeapMemory;
    private final boolean commitChaining;
    private final Options tableOption;

    public StpFlinkCdcMultiDynamicBucketTableSink(
            Catalog.Loader catalogLoader,
            double commitCpuCores,
            @Nullable MemorySize commitHeapMemory,
            boolean commitChaining,
            Options tableOption) {
        this.catalogLoader = catalogLoader;
        this.commitCpuCores = commitCpuCores;
        this.commitHeapMemory = commitHeapMemory;
        this.commitChaining = commitChaining;
        this.tableOption = tableOption;
    }

    private StoreSinkWrite.WithWriteBufferProvider createWriteProvider() {
        // for now, no compaction for multiplexed sink
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new StoreSinkWriteImpl(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        isOverwrite,
                        table.coreOptions().prepareCommitWaitCompaction(),
                        true,
                        memoryPoolFactory,
                        metricGroup);
    }

    public DataStreamSink<?> sinkFrom(DataStream<CdcMultiplexRecord> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser, createWriteProvider());
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<CdcMultiplexRecord> input,
            String commitUser,
            StoreSinkWrite.WithWriteBufferProvider sinkProvider) {
        int parallelism = input.getParallelism();
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        assertStreamingConfiguration(env);

        // Abstract topology:
        // input -- shuffle by key hash --> bucket-assigner -- shuffle by partition & bucket -->
        // writer --> committer

        // 1. shuffle by key hash
        Integer assignerParallelism = input.getParallelism();

        Integer numAssigners = tableOption.getInteger(CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), 1);
        DataStream<CdcMultiplexRecord> partitionByKeyHash =
                partition(input, assignerChannelComputer(numAssigners), assignerParallelism);

        // 2. bucket-assigner
        StpMultiTableHashBucketAssignerOperator assignerOperator =
                new StpMultiTableHashBucketAssignerOperator(
                        commitUser, catalogLoader, numAssigners, extractorFunction(), false, -1);
        TupleTypeInfo<Tuple2<CdcMultiplexRecord, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(partitionByKeyHash.getType(), BasicTypeInfo.INT_TYPE_INFO);
        DataStream<Tuple2<CdcMultiplexRecord, Integer>> bucketAssigned =
                partitionByKeyHash
                        .transform("dynamic-bucket-assigner", rowWithBucketType, assignerOperator)
                        .setParallelism(partitionByKeyHash.getParallelism());

        // 3. shuffle by partition & bucket
        DataStream<Tuple2<CdcMultiplexRecord, Integer>> partitionByBucket =
                partition(bucketAssigned, channelComputer2(), parallelism);

        MultiTableCommittableTypeInfo typeInfo = new MultiTableCommittableTypeInfo();
        SingleOutputStreamOperator<MultiTableCommittable> written =
                partitionByBucket
                        .transform(
                                WRITER_NAME,
                                typeInfo,
                                createWriteOperator(sinkProvider, commitUser))
                        .setParallelism(input.getParallelism());

        // shuffle committables by table
        DataStream<MultiTableCommittable> partitioned =
                FlinkStreamPartitioner.partition(
                        written,
                        new MultiTableCommittableChannelComputer(),
                        input.getParallelism());

        SingleOutputStreamOperator<?> committed =
                partitioned
                        .transform(
                                GLOBAL_COMMITTER_NAME,
                                typeInfo,
                                new CommitterOperator<>(
                                        true,
                                        false,
                                        commitChaining,
                                        commitUser,
                                        createCommitterFactory(),
                                        createCommittableStateManager()))
                        .setParallelism(input.getParallelism());
        configureGlobalCommitter(committed, commitCpuCores, commitHeapMemory);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    private SerializableFunction<TableSchema, PartitionKeyExtractor<CdcMultiplexRecord>>
    extractorFunction() {
        return schema -> {
            KeyAndBucketExtractor<CdcMultiplexRecord> extractor = createExtractor(schema);
            return new PartitionKeyExtractor<CdcMultiplexRecord>() {
                @Override
                public BinaryRow partition(CdcMultiplexRecord record) {
                    extractor.setRecord(record);
                    return extractor.partition();
                }

                @Override
                public BinaryRow trimmedPrimaryKey(CdcMultiplexRecord record) {
                    extractor.setRecord(record);
                    return extractor.trimmedPrimaryKey();
                }
            };
        };
    }

    private ChannelComputer<CdcMultiplexRecord> assignerChannelComputer(Integer numAssigners) {
        return new AssignerChannelComputer(numAssigners, catalogLoader);
    }

    private ChannelComputer<Tuple2<CdcMultiplexRecord, Integer>> channelComputer2() {
        return new RecordWithBucketChannelComputer(this.catalogLoader);
    }

    protected OneInputStreamOperator<Tuple2<CdcMultiplexRecord, Integer>, MultiTableCommittable>
    createWriteOperator(
            StoreSinkWrite.WithWriteBufferProvider writeProvider, String commitUser) {
        return new StpCdcRecordStoreDynamicBucketMultiWriteOperator(
                catalogLoader,
                writeProvider,
                commitUser,
                Optional.ofNullable(this.tableOption).orElse(new Options()));
    }

    // Table committers are dynamically created at runtime
    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
    createCommitterFactory() {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return context -> new StoreMultiCommitter(catalogLoader, context);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                WrappedManifestCommittableSerializer::new);
    }

    private class AssignerChannelComputer implements ChannelComputer<CdcMultiplexRecord> {
        private final Catalog.Loader loader;
        private Integer numAssigners;

        private transient int numChannels;
        private Map<Identifier, KeyAndBucketExtractor<CdcMultiplexRecord>> extractors;

        public AssignerChannelComputer(Integer numAssigners, Catalog.Loader loader) {
            this.numAssigners = numAssigners;
            this.loader = loader;
        }

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
            this.numAssigners = MathUtils.min(numAssigners, numChannels);
            this.extractors = new HashMap<>();
        }

        @Override
        public int channel(CdcMultiplexRecord record) {
            Identifier identifier = Identifier.create(record.databaseName(), record.tableName());
            if (!extractors.containsKey(identifier)) {
                Table table;
                try {
                    try (Catalog catalog = loader.load()) {
                        table = catalog.getTable(identifier);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                KeyAndBucketExtractor<CdcMultiplexRecord> extractor =
                        createExtractor(((FileStoreTable) table).schema());
                extractors.put(identifier, extractor);
            }
            KeyAndBucketExtractor<CdcMultiplexRecord> extractor = extractors.get(identifier);

            extractor.setRecord(record);
            int partitionHash = extractor.partition().hashCode();
            int keyHash = extractor.trimmedPrimaryKey().hashCode();
            return computeAssigner(partitionHash, keyHash, numChannels, numAssigners);
        }

        @Override
        public String toString() {
            return "shuffle by key hash";
        }
    }

    private class RecordWithBucketChannelComputer
            implements ChannelComputer<Tuple2<CdcMultiplexRecord, Integer>> {
        private transient int numChannels;
        private final Catalog.Loader catalogLoader;
        private transient Map<Identifier, KeyAndBucketExtractor<CdcMultiplexRecord>> extractors;
        private transient Map<Identifier, FileStoreTable> tables;
        private transient Catalog catalog;

        public RecordWithBucketChannelComputer(Catalog.Loader catalogLoader) {
            this.catalogLoader = catalogLoader;
        }

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
            this.extractors = new HashMap<>();
            this.tables = new HashMap<>();
            this.catalog = catalogLoader.load();
        }

        @Override
        public int channel(Tuple2<CdcMultiplexRecord, Integer> record) {
            CdcMultiplexRecord cdcRecord = record.f0;
            Identifier identifier =
                    Identifier.create(cdcRecord.databaseName(), cdcRecord.tableName());

            FileStoreTable table;
            try {
                table = tables.computeIfAbsent(identifier, id -> {
                    try {
                        return (FileStoreTable) catalog.getTable(identifier);
                    } catch (Catalog.TableNotExistException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!extractors.containsKey(identifier)) {
                KeyAndBucketExtractor<CdcMultiplexRecord> extractor =
                        createExtractor(table.schema());
                extractors.put(identifier, extractor);
            }
            KeyAndBucketExtractor<CdcMultiplexRecord> extractor = extractors.get(identifier);
            extractor.setRecord(record.f0);
            return ChannelComputer.select(extractor.partition(), record.f1, numChannels);
        }

        @Override
        public String toString() {
            return "shuffle by partition & bucket";
        }
    }

    protected KeyAndBucketExtractor<CdcMultiplexRecord> createExtractor(TableSchema schema) {
        return new StpCdcMutiplexRecordKeyAndBucketExtractor(schema);
    }
}
