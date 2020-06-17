package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

public class ChangeCacheEncryptionRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request id. */
    private final UUID reqId;

    /** Encrypted master key name. */
    private final int[] groups;

    /** Encryption keys. */
    private final byte[][] keys;

    private final byte[] keyIds;

    private AffinityTopologyVersion topVer;

    /**
     * @param groups Groups.
     * @param keys Keys.
     */
    public ChangeCacheEncryptionRequest(int[] groups, byte[][] keys, byte[] keyIds, AffinityTopologyVersion topVer) {
        reqId = UUID.randomUUID();

        this.groups = groups;
        this.keys = keys;
        this.keyIds = keyIds;
        this.topVer = topVer;
    }

    public UUID requestId() {
        return this.reqId;
    }

    public int[] groups() {
        return groups;
    }

    public byte[][] keys() {
        return keys;
    }

    public byte[] keyIdentifiers() { return keyIds; }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return Objects.equals(reqId, ((ChangeCacheEncryptionRequest)o).reqId);
    }

    @Override public int hashCode() {
        return Objects.hash(reqId);
    }
}
