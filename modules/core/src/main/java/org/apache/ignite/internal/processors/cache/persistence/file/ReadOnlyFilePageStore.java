package org.apache.ignite.internal.processors.cache.persistence.file;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;

// todo remove?
public class ReadOnlyFilePageStore implements PageStore {
    @Override public boolean exists() {
        return false;
    }

    @Override public long allocatePage() throws IgniteCheckedException {
        return 0;
    }

    @Override public int pages() {
        return 0;
    }

    @Override public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {

    }

    @Override public void readHeader(ByteBuffer buf) throws IgniteCheckedException {

    }

    @Override
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException {

    }

    @Override public long pageOffset(long pageId) {
        return 0;
    }

    @Override public void sync() throws IgniteCheckedException {

    }

    @Override public void ensure() throws IgniteCheckedException {

    }

    @Override public int version() {
        return 0;
    }

    @Override public void stop(boolean cleanFile) throws StorageException {

    }

    @Override public void beginRecover() {

    }

    @Override public void finishRecover() throws StorageException {

    }

    @Override public void truncate(int tag) throws StorageException {

    }

    @Override public int getPageSize() {
        return 0;
    }

    @Override public int getBlockSize() {
        return 0;
    }

    @Override public long size() {
        return 0;
    }

    @Override public long getSparseSize() {
        return 0;
    }

    @Override public void punchHole(long pageId, int usefulBytes) {

    }
}
