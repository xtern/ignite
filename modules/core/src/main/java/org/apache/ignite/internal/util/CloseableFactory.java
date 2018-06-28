package org.apache.ignite.internal.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;

/** */
public final class CloseableFactory<T> implements Factory<T>, Closeable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Factory<T> origin;

    /** */
    private transient final Set<Closeable> set;

    /** */
    public CloseableFactory(final Factory<T> origin) {
        this.origin = origin;
        this.set = new GridConcurrentHashSet<>();
    }

    /** {@inheritDoc} */
    @Override public T create() {
        T val = origin.create();

        if (val != null && val instanceof Closeable)
            set.add((Closeable) val);

        return val;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (origin instanceof Closeable)
            ((Closeable) origin).close();
        else {
            for (Closeable obj : set) {
                try {
                    obj.close();
                }
                catch (IOException e) {
                    // No-op.
                }
            }
        }
    }
}
