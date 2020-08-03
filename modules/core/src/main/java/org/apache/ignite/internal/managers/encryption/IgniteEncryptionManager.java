/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFinishedFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Manages cache keys and {@code EncryptionSpi} instances in in-memory grid.
 */
public class IgniteEncryptionManager extends GridManagerAdapter<EncryptionSpi> implements IgniteEncryption {
    /**
     * @param ctx  Kernal context.
     * @param spis Specific SPI instance.
     */
    public IgniteEncryptionManager(GridKernalContext ctx, EncryptionSpi... spis) {
        super(ctx, ctx.config().getEncryptionSpi());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> changeMasterKey(String masterKeyName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getMasterKeyName() {
        throw new UnsupportedOperationException();
    }

    /**
     * Digest of last changed master key or {@code null} if master key was not changed.
     * <p>
     * Used to verify the digest on a client node in case of cache start after master key change.
     *
     * @return Digest of last changed master key or {@code null} if master key was not changed.
     */
    public byte[] masterKeyDigest() {
        throw new UnsupportedOperationException();
    }

    /** @return {@code True} if the master key change process in progress. */
    public boolean isMasterKeyChangeInProgress() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Returns group encryption key.
     *
     * @param grpId Group id.
     * @return Group encryption key.
     */
    @Nullable public Serializable groupKey(int grpId) {
        return null;
    }

    /**
     * @param keyCnt Count of keys to generate.
     * @return Future that will contain results of generation.
     */
    public IgniteInternalFuture<T2<Collection<byte[]>, byte[]>> generateKeys(int keyCnt) {
        return new GridDhtFinishedFuture<>(new UnsupportedOperationException());
    }

    /**
     * Checks cache encryption supported by all nodes in cluster.
     *
     * @throws IgniteCheckedException If check fails.
     */
    public void checkEncryptedCacheSupported() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Callback for cache group destroy event.
     *
     * @param grpId Cache group ID.
     */
    public void onCacheGroupDestroyed(int grpId) {
        // No-op.
    }

    public void onActivate(GridKernalContext kctx) {

    }

    public void onDeActivate(GridKernalContext kctx) {

    }

    public void onLocalJoin() {

    }

    public void beforeCacheGroupStart(int grpId, @Nullable byte[] encKey) {

    }

    /**
     * Apply keys from WAL record during the recovery phase.
     *
     * @param rec Record.
     */
    public void applyKeys(MasterKeyChangeRecord rec) {
        throw new UnsupportedOperationException();
    }
}
