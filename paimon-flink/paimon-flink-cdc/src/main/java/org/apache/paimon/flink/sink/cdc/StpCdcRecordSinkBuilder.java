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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.utils.SingleOutputStreamOperatorUtils;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/**
 * Builder for CDC {@link FlinkWriteSink} when syncing the custom paimon tables. Each table will be
 * written into a separate Paimon table.
 *
 * <p>This builder will create a separate sink for each Paimon sink table. Thus this implementation
 * is not very efficient in resource saving.
 *
 * <p>For newly added tables, this builder will create a multiplexed Paimon sink to handle all
 * tables added during runtime. Note that the topology of the Flink job is likely to change when
 * there is newly added table and the job resume from a given savepoint.
 *
 * @param <T> CDC change event type
 */
public class StpCdcRecordSinkBuilder<T> implements Serializable {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory;

    @Nullable private Integer parallelism;
    private double committerCpu;
    @Nullable private MemorySize committerMemory;
    private boolean commitChaining;

    // Paimon catalog used to check and create tables. There will be two
    //     places where this catalog is used. 1) in processing function,
    //     it will check newly added tables and create the corresponding
    //     Paimon tables. 2) in multiplex sink where it is used to
    //     initialize different writers to multiple tables.
    private Catalog.Loader catalogLoader;

    public StpCdcRecordSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public StpCdcRecordSinkBuilder<T> withParserFactory(EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public StpCdcRecordSinkBuilder<T> withTableOptions(Options options) {
        this.parallelism = options.get(FlinkConnectorOptions.SINK_PARALLELISM);
        this.committerCpu = options.get(FlinkConnectorOptions.SINK_COMMITTER_CPU);
        this.committerMemory = options.get(FlinkConnectorOptions.SINK_COMMITTER_MEMORY);
        this.commitChaining = options.get(FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING);
        return this;
    }

    public StpCdcRecordSinkBuilder<T> withCatalogLoader(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(catalogLoader);

        buildCombinedCdcSink();
    }

    private void buildCombinedCdcSink() {
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(
                                new StpCdcDynamicTableParsingProcessFunction<>(
                                        catalogLoader, parserFactory))
                        .name("Side Output")
                        .setParallelism(input.getParallelism());
        DataStream<CdcMultiplexRecord> fixedBucketDS =
                SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed, StpCdcDynamicTableParsingProcessFunction.DYNAMIC_OUTPUT_TAG);

        DataStream<CdcMultiplexRecord> dynamicBucketDS =
                SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed, StpCdcDynamicTableParsingProcessFunction.DYNAMIC_BUCKET_OUTPUT_TAG);

        DataStream<CdcMultiplexRecord> unawareBucketDS =
                SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed, StpCdcDynamicTableParsingProcessFunction.UNAWARE_BUCKET_OUTPUT_TAG);

        SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed,
                        CdcDynamicTableParsingProcessFunction.DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG)
                .keyBy(t -> t.f0)
                .process(new MultiTableUpdatedDataFieldsProcessFunction(catalogLoader))
                .name("Schema Evolution");

        DataStream<CdcMultiplexRecord> partitioned =
                partition(
                        fixedBucketDS,
                        //  only fixed bucket supported
                        new CdcMultiplexRecordChannelComputer(catalogLoader),
                        parallelism);

        new FlinkCdcMultiTableSink(catalogLoader, committerCpu, committerMemory, commitChaining)
                .sinkFrom(partitioned);

        new StpFlinkCdcMultiDynamicBucketTableSink(
                        catalogLoader, committerCpu, committerMemory, commitChaining)
                .sinkFrom(dynamicBucketDS);

        new StpFlinkCdcMultiUnawareBucketTableSink(
                        catalogLoader, committerCpu, committerMemory, commitChaining)
                .sinkFrom(unawareBucketDS);
    }
}
