package org.apache.ignite.internal.processors.diag;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum DiagnosticTopics {
    /** Root. */
    TOTAL("# rebalance total"),
    /** GridDhtPartitionDemander#preloadEntry(..) */
    PRELOAD_ENTRY("## preload entry total"),
    /** GridCacheMapEntry#storeValue(..) */
    PRELOAD_OFFHEAP_INVOKE("### initialValue(..) -> offheap().invoke(..)"),
    /** CacheDataStoreImpl#invoke0(..) */
    PRELOAD_TREE_INVOKE("#### tree.invoke(..)"),
    /** */
    PRELOAD_TREE_ADD_ROW("#### rowStore.addRow(..)"),

    /** CacheDataStoreImpl.finishUpdate(..) */
    PRELOAD_INDEXING_STORE("#### finishUpdate -> indexing().store(..)"),
    /** CacheDataStoreImpl.finishUpdate(..) */
    PRELOAD_PENDING_TREE_REMOVE("#### finishUpdate -> pendingTree().removex(..)"),
    /** CacheDataStoreImpl.finishUpdate(..) */
    PRELOAD_PENDING_TREE_PUT("#### finishUpdate -> pendingTree().putx(..)"),
    /** CacheDataStoreImpl#finishRemove(..) */
    PRELOAD_INDEXING_REMOVE("#### finishRemove -> indexing().remove(..)"),
    /** CacheDataStoreImpl#finishRemove(..) */
    PRELOAD_FREELIST_REMOVE("#### finishRemove -> freeList.removeDataRowByLink(..)"),
    /** */
    PRELOAD_UPDATED("### initialValue(..) -> GridCacheMapEntry.updated(..)"),
    /** */
    PRELOAD_ON_WAL_LOG("### initialValue(..) -> wal.log(..)"),
    /** */
    PRELOAD_ON_ENTRY_UPDATED("### initialValue(..) -> cq().onEntryUpdated(..)"),

    SEND_DEMAND("# demand message serialization"),
    SEND_RECEIVE("# network delay between nodes"),
    SUPPLIER_PROCESS_MSG("# prepare message supplier");

    /** Reverse-lookup map for getting a day from an abbreviation */
    private static final Map<String, DiagnosticTopics> lookup = new HashMap<String, DiagnosticTopics>();

    static {
        for (DiagnosticTopics t : DiagnosticTopics.values())
            lookup.put(t.getName(), t);
    }

    /** */
    private String name;

    /** */
    DiagnosticTopics(String name) {
        this.name = name;
    }

    /** */
    public static DiagnosticTopics get(String topic) {
        return lookup.get(topic);
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public DiagnosticTopics setName(String name) {
        this.name = name;
        return this;
    }
}
