package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

public class SnapshotRestoreRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private final String snpName;

    /** The list of cache groups to include into snapshot. */
    @GridToStringInclude
    private final Collection<String> grps;

    /** Request ID. */
    private final UUID reqId = UUID.randomUUID();

    public SnapshotRestoreRequest(String snpName, Collection<String> grps) {
        this.snpName = snpName;
        this.grps = grps;
    }

    public UUID requestId() {
        return reqId;
    }

    public Collection<String> groups() {
        return grps;
    }

    public String snapshotName() {
        return snpName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return Objects.equals(reqId, ((SnapshotRestoreRequest)o).reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(reqId);
    }
}
