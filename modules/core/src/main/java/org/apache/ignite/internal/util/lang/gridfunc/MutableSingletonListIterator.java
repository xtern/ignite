/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 *
 * @param <T>
 */
public class MutableSingletonListIterator<T> implements ListIterator<T> {
    /** */
    private boolean hasNext = true;

    /** */
    private T item;

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        if (!hasNext)
            throw new NoSuchElementException();

        hasNext = false;

        return item;
    }

    /** {@inheritDoc} */
    @Override public boolean hasPrevious() {
        return !hasNext;
    }

    /** {@inheritDoc} */
    @Override public T previous() {
        if (hasNext)
            throw new NoSuchElementException();

        hasNext = true;

        return item;
    }

    /** {@inheritDoc} */
    @Override public int nextIndex() {
        return hasNext ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public int previousIndex() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void set(T item) {
        this.item = item;

        hasNext = true;
    }

    /** {@inheritDoc} */
    @Override public void add(T t) {
        throw new UnsupportedOperationException();
    }

}
