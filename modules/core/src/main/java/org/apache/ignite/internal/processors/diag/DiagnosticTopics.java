package org.apache.ignite.internal.processors.diag;

/**
 *
 */
public enum DiagnosticTopics {
    SEND_DEMAND("demand message serialization"),
    SEND_RECEIVE("network delay between nodes"),
    CLEAR_FUTS("cache wait clearAllFutures"),
    TOTAL("total"),
    PRELOAD_ENTRY("preload entries total"),
    PRELOAD_ON_ENTRY_UPDATED("preload entry onEntryUpdated"),
    PRELOAD_ON_WAL_LOG("preload entry wal log"),
    PRELOAD_STORE_ENTRY("preload entry tree invoke"),
    PRELOAD_UPDATED("preload entry updated"),
    SUPPLIER_PROCESS_MSG("prepare message by supplier");

    /** */
    private String name;

    /** */
    DiagnosticTopics(String name) {
        this.name = name;
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
