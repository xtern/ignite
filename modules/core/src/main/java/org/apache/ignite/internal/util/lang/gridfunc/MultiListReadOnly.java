package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/** todo implement list */
public class MultiListReadOnly<T> implements Collection<T> {
    /** todo atomic? */
    private int size;

    /** */
    private final List<ArrayList<T>> cols;

    public MultiListReadOnly(int segments) {
        cols  = new ArrayList<>(segments);

        // todo lazy
        for (int i = 0; i < segments; i++)
            cols.add(new ArrayList<>(4096/segments));
    }

    @Override public int size() {
        return size;
    }

    @Override public boolean isEmpty() {
        return size != 0;
    }

    @Override public boolean contains(Object o) {
        for (List list : cols) {
            if (list.contains(o))
                return true;
        }

        return false;
    }

    @NotNull @Override public Iterator<T> iterator() {
        return new MultipleIterator<T>(new TransformCollectionView<Iterator<T>, Iterable<T>>(cols, Iterable::iterator).iterator());
    }

    @NotNull @Override public Object[] toArray() {
        throw new UnsupportedOperationException();
        //return new Object[0];
    }

    @NotNull @Override public <T1> T1[] toArray(@NotNull T1[] a) {
        throw new UnsupportedOperationException();
        //return null;
    }

    /** */
    public void add(int idx, T key) {
        cols.get(idx).add(key);

        ++size;
    }

    /** */
    public void addAll(int idx, Collection<T> keys) {
        cols.get(idx).addAll(keys);

        size += keys.size();
    }

    @Override public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean containsAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(@NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public void clear() {
        throw new UnsupportedOperationException();
    }
}

