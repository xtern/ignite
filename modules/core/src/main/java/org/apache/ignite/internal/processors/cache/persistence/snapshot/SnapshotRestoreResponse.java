package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.CacheConfiguration;

public class SnapshotRestoreResponse implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private Map<String, CacheGroupSnapshotDetails> locParts;

    void put(String name, CacheConfiguration<?, ?> cfg, Set<Integer> parts) {
        if (locParts == null)
            locParts = new HashMap<>();

        locParts.put(name, new CacheGroupSnapshotDetails(cfg, parts));
    }

    Map<String, CacheGroupSnapshotDetails> locParts() {
        return locParts;
    }

    static class CacheGroupSnapshotDetails implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        private CacheConfiguration cfg;
        private Set<Integer> parts;

        public CacheGroupSnapshotDetails(CacheConfiguration cfg, Set<Integer> parts) {
            this.cfg = cfg;
            this.parts = parts;
        }

        public CacheConfiguration config() {
            return cfg;
        }

        public Set<Integer> parts() {
            return parts;
        }
    }
}
