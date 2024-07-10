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

package org.apache.paimon.index;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Assign bucket for key hashcode.
 */
public class StpHashBucketAssigner implements BucketAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(StpHashBucketAssigner.class);

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final IndexFileHandler indexFileHandler;
    private final int numChannels;
    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;

    private final Cache<BinaryRow, PartitionIndex> partitionIndex;
    private final long indexExpireTimestamp;

    public StpHashBucketAssigner(
            SnapshotManager snapshotManager,
            String commitUser,
            IndexFileHandler indexFileHandler,
            int numChannels,
            int numAssigners,
            int assignId,
            long targetBucketRowNumber,
            long indexExpireTimestamp) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.indexFileHandler = indexFileHandler;
        this.numChannels = numChannels;
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.indexExpireTimestamp = indexExpireTimestamp;
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (indexExpireTimestamp > 0) {
            builder.expireAfterAccess(indexExpireTimestamp, TimeUnit.MILLISECONDS);
        }
        this.partitionIndex = builder
                .removalListener((o, o2, removalCause) -> {
                    if (removalCause.wasEvicted()) {
                        LOG.info("Partition {} expire.", o);
                    }
                })
                .build();
    }

    /**
     * Assign a bucket for key hash of a record.
     */
    @Override
    public int assign(BinaryRow partition, int hash) {
        int partitionHash = partition.hashCode();
        int recordAssignId = computeAssignId(partitionHash, hash);
        checkArgument(
                recordAssignId == assignId,
                "This is a bug, record assign id %s should equal to assign id %s.",
                recordAssignId,
                assignId);

        PartitionIndex index = this.partitionIndex.getIfPresent(partition);
        if (index == null) {
            partition = partition.copy();
            index = loadIndex(partition, partitionHash);
            this.partitionIndex.put(partition, index);
        }

        int assigned = index.assign(hash, this::isMyBucket);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Assign " + assigned + " to the partition " + partition + " key hash " + hash);
        }
        return assigned;
    }

    /**
     * Prepare commit to clear outdated partition index.
     */
    @Override
    public void prepareCommit(long commitIdentifier) {
        long latestCommittedIdentifier;
        ConcurrentMap<@NonNull BinaryRow, @NonNull PartitionIndex> map = partitionIndex.asMap();
        if (map.values().stream()
                .mapToLong(i -> i.lastAccessedCommitIdentifier)
                .max()
                .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // Optimization for the first commit.
            //
            // If this is the first commit, no index has previous modified commit, so the value of
            // `latestCommittedIdentifier` does not matter.
            //
            // Without this optimization, we may need to scan through all snapshots only to find
            // that there is no previous snapshot by this user, which is very inefficient.
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            latestCommittedIdentifier =
                    snapshotManager
                            .latestSnapshotOfUser(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        Iterator<Map.Entry<BinaryRow, PartitionIndex>> iterator =
                map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BinaryRow, PartitionIndex> entry = iterator.next();
            BinaryRow partition = entry.getKey();
            PartitionIndex index = entry.getValue();
            if (index.accessed) {
                index.lastAccessedCommitIdentifier = commitIdentifier;
            } else {
                if (index.lastAccessedCommitIdentifier <= latestCommittedIdentifier) {
                    // Clear writer if no update, and if its latest modification has committed.
                    //
                    // We need a mechanism to clear index, otherwise there will be more and
                    // more such as yesterday's partition that no longer needs to be accessed.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Removing index for partition {}. "
                                        + "Index's last accessed identifier is {}, "
                                        + "while latest committed identifier is {}, "
                                        + "current commit identifier is {}.",
                                partition,
                                index.lastAccessedCommitIdentifier,
                                latestCommittedIdentifier,
                                commitIdentifier);
                    }
                    iterator.remove();
                }
            }
            index.accessed = false;
        }
    }

    @VisibleForTesting
    Set<BinaryRow> currentPartitions() {
        return partitionIndex.asMap().keySet();
    }

    private int computeAssignId(int partitionHash, int keyHash) {
        return BucketAssigner.computeAssigner(partitionHash, keyHash, numChannels, numAssigners);
    }

    private boolean isMyBucket(int bucket) {
        return bucket % numAssigners == assignId % numAssigners;
    }

    private PartitionIndex loadIndex(BinaryRow partition, int partitionHash) {
        return PartitionIndex.loadIndex(
                indexFileHandler,
                partition,
                targetBucketRowNumber,
                (hash) -> computeAssignId(partitionHash, hash) == assignId,
                this::isMyBucket);
    }
}
