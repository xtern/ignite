package org.apache.ignite.internal.processors.diag;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum DiagnosticTopics {
    /** Root. */

//    /** GridDhtPartitionDemander#preloadEntry(..) */
//    PRELOAD_ENTRY("# # preload on demander"),
//    /** GridCacheMapEntry#storeValue(..) */
//    PRELOAD_OFFHEAP_INVOKE("# # # offheap().invoke(..)"),
//    /** CacheDataStoreImpl#invoke0(..) */
//    PRELOAD_TREE_INVOKE("# # # # dataTree.invoke(..)"),
//    /** rowStore.addRow(..) */
//    PRELOAD_TREE_ADD_ROW("# # # # # FreeList.insertDataRow(..)"),
//    /** */
//    PRELOAD_TREE_FINISH_UPDATE("# # # # CacheDataStoreImpl.finishUpdate(..)"),
//    /** CacheDataStoreImpl.finishUpdate(..) */
//    PRELOAD_INDEXING_STORE("# # # # # indexing().store(..)"),
//    /** CacheDataStoreImpl.finishUpdate(..) */
//    PRELOAD_PENDING_TREE_REMOVE("# # # # # pendingTree().removex(..)"),
//    /** CacheDataStoreImpl.finishUpdate(..) */
//    PRELOAD_PENDING_TREE_PUT("# # # # # pendingTree().putx(..)"),
//    /** CacheDataStoreImpl#finishRemove(..) */
//    PRELOAD_INDEXING_REMOVE("# # # # finishRemove -> indexing().remove(..)"),
//    /** CacheDataStoreImpl#finishRemove(..) */
//    PRELOAD_FREELIST_REMOVE("# # # # finishRemove -> freeList.removeDataRowByLink(..)"),
//    /** */
//    PRELOAD_UPDATED("# # # ttl().addTrackedEntry(..)"),
//    /** */
//    PRELOAD_ON_WAL_LOG("# # # wal.log(..)"),
//    /** */
//    PRELOAD_ON_ENTRY_UPDATED("# # # continuousQueries().onEntryUpdated(..)"),
//
//    SEND_DEMAND("# message serialization"),
//    SEND_RECEIVE("# network delay between nodes"),
//    SUPPLIER_PROCESS_MSG("# make batch on supplier handleDemandMessage(..)"),
    DEMANDER_PROCESS_MSG("# demander handleSupplyMessage(..)"),
    TOTAL("# cache rebalance total");

    /** */
    private String desc;

    /** */
    DiagnosticTopics(String desc) {
        this.desc = desc;
    }

    /** */
    public String desc() {
        return desc;
    }
}
