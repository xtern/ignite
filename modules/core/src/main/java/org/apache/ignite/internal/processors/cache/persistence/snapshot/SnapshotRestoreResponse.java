package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.CacheConfiguration;

public class SnapshotRestoreResponse implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private Map<String, CacheGroupSnapshotDetails> locParts;

    void put(String name, CacheGroupSnapshotDetails grpDetails) {
        if (locParts == null)
            locParts = new HashMap<>();

        locParts.put(name, grpDetails);
    }

    Map<String, CacheGroupSnapshotDetails> locParts() {
        return locParts;
    }

    static class CacheGroupSnapshotDetails implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        private List<CacheConfiguration<?, ?>> cfgs;
        private Set<Integer> parts;

        public CacheGroupSnapshotDetails(List<CacheConfiguration<?, ?>> cfgs, Set<Integer> parts) {
            this.cfgs = cfgs;
            this.parts = parts;
        }

        public List<CacheConfiguration<?, ?>> configs() {
            return cfgs;
        }

        public Set<Integer> parts() {
            return parts;
        }
    }
}
