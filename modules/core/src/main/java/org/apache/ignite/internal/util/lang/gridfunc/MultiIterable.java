package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/** */
public class MultiIterable<T> implements Iterable<T> {
    /** */
    private final List<ArrayList<T>> cols;

    /** */
    private int size;

    /** */
    public MultiIterable(int segments) {
        cols  = new ArrayList<>(segments);

        for (int i = 0; i < segments; i++)
            cols.add(new ArrayList<>());
    }

    /** */
    public void put(int idx, T key) {
        cols.get(idx).add(key);

        ++size;
    }

    /** todo smarter implementation */
    public int size() {
        return size;
    }

    /** */
    @NotNull @Override public Iterator<T> iterator() {
        return new MultipleIterator<T>(
            new TransformCollectionView<Iterator<T>, Iterable<T>>(
                cols, Iterable::iterator)
                .iterator()
        );
    }
}
