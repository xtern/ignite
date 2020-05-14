package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;

public class ChangeCacheEncryptionRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request id. */
    private final UUID reqId;

    /** Encrypted master key name. */
    private final Collection<Integer> groups;

    /** Encryption keys. */
    private final byte[][] keys;

    /**
     * @param groups Groups.
     * @param keys Keys.
     */
    public ChangeCacheEncryptionRequest(Collection<Integer> groups, byte[][] keys) {
        reqId = UUID.randomUUID();

        this.groups = groups;
        this.keys = keys;
    }

    public UUID requestId() {
        return this.reqId;
    }

    public Collection<Integer> groups() {
        return groups;
    }

    public byte[][] keys() {
        return keys;
    }
}
