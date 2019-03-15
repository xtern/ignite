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
//
//    PRELOAD_OFFHEAP_INVOKE_INSERT_FREELIST("# # # freeList.insertDataRow"),


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
//    DEMAND_MSG_SEND("# # demand message send"),
//    SUPPLY_MSG_SEND("# # supply message send"),
    SUPPLIER_PROCESS_MSG("# make batch on supplier handleDemandMessage(..)"),

    DEMANDER_PROCESS_MSG_SINGLE("# # demander process single"),
//    DEMANDER_PROCESS_MSG_BATCH_BIN_SEARCH("# # # # # demander search freelist"),
//    DEMANDER_PROCESS_MSG_BATCH_BIN_PACK("# # # # # demander process binPack"),
//    DEMANDER_PROCESS_MSG_BATCH_BIN_INSERT("# # # # # demander process insert"),
//    DEMANDER_PROCESS_MSG_BATCH_ALLOC_PAGE("# # # # # demander alloc page"),
//    PRELOAD_OFFHEAP_BATCH_FIND("# # # # # demander find"),
//    PRELOAD_OFFHEAP_BATCH_INSERT("# # # # demander rowStore.freeList().insertBatch"),
//    PRELOAD_OFFHEAP_BATCH_TREE_INSERT("# # # # demander dataTree.putx"),
//    DEMANDER_PROCESS_MSG_BATCH_LOCK("# # # batch lock"),
//    DEMANDER_PROCESS_MSG_BATCH_UNLOCK("# # # batch unlock"),
//    DEMANDER_PROCESS_MSG_BATCH_UPDATE("# # # demander batch update"),
    DEMANDER_PROCESS_MSG_BATCH("# # demander process batch"),

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
