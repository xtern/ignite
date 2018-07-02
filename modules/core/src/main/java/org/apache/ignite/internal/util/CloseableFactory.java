package org.apache.ignite.internal.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import org.apache.ignite.internal.util.typedef.internal.U;

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

        if (val != null && val instanceof Closeable) {
            System.out.println("add closeable " + val);

            set.add((Closeable)val);
        }
        else
            if (val != null)
                System.out.println("not closeable " + val);

        return val;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (origin instanceof Closeable) {
            System.out.println(">xxx> origin close: " + origin);

            ((Closeable)origin).close();
        } else {
            if (set.isEmpty())
                System.out.println(">xxx> nothing to cleanup");
            else
            for (Closeable obj : set) {
                try {
                    System.out.println("obj close " + obj);
                    obj.close();
                }
                catch (IOException e) {
                    // No-op.
                }
            }
        }
    }
}
