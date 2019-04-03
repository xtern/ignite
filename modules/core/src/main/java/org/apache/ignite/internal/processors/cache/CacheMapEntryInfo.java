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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheMapEntries.BatchContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;

/**
 *
 */
public class CacheMapEntryInfo {
    /** */
    private BatchContext ctx;

    /** */
    private final KeyCacheObject key;

    /** */
    private final CacheObject val;

    /** */
    private final long expireTime;

    /** */
    private final long ttl;

    /** */
    private final GridCacheVersion ver;

    /** */
    private final GridDrType drType;

    /** */
    private GridDhtCacheEntry entry;

    /** */
    private boolean update;

    /**
     *
     */
    public CacheMapEntryInfo(
        KeyCacheObject key,
        CacheObject val,
        long expireTime,
        long ttl,
        GridCacheVersion ver,
        GridDrType drType
    ) {
        assert key != null;

        this.key = key;
        this.val = val;
        this.ver = ver;
        this.ttl = ttl;
        this.drType = drType;
        this.expireTime = expireTime;
    }

    /**
     *
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     *
     */
    public void onRemove() {
        ctx.markRemoved(key);
    }

    /**
     * @return Cache entry.
     */
    GridDhtCacheEntry cacheEntry() {
        return entry;
    }

    /**
     *
     */
    void init(BatchContext ctx, GridDhtCacheEntry entry) {
        assert ctx != null;
        assert entry != null;

        this.ctx = ctx;
        this.entry = entry;
    }

    /**
     *
     */
    void updateCacheEntry() throws IgniteCheckedException {
        if (!update)
            return;

        entry.finishInitialUpdate(val, expireTime, ttl, ver, ctx.topVer(), drType, null, ctx.preload());
    }

    /**
     *
     */
    boolean needUpdate(CacheDataRow row) throws GridCacheEntryRemovedException {
        GridCacheVersion currVer = row != null ? row.version() : entry.version();

        GridCacheContext cctx = ctx.context();

        boolean isStartVer = cctx.versions().isStartVersion(currVer);

        boolean update0;

        if (cctx.group().persistenceEnabled()) {
            if (!isStartVer) {
                if (cctx.atomic())
                    update0 = GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(currVer, version()) < 0;
                else
                    update0 = currVer.compareTo(version()) < 0;
            }
            else
                update0 = true;
        }
        else
            update0 = (isStartVer && row == null);

        update0 |= (!ctx.preload() && entry.deletedUnlocked());

        update = update0;

        return update0;
    }
}
