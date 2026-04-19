package com.apollo.storage;

import java.util.Arrays;
import java.util.Objects;

public class Page {
    public enum PageType {
        DATA,
        INDEX,
        OVERFLOW,
        FREE
    }

    private final long pageId;
    private final PageType type;
    private final byte[] data;
    private long lsn;
    private boolean dirty;

    public Page(long pageId, PageType type, int pageSize) {
        this.pageId = pageId;
        this.type = Objects.requireNonNull(type, "type");
        this.data = new byte[pageSize];
    }

    public Page(long pageId, PageType type, byte[] data, long lsn) {
        this.pageId = pageId;
        this.type = Objects.requireNonNull(type, "type");
        this.data = Arrays.copyOf(Objects.requireNonNull(data, "data"), data.length);
        this.lsn = lsn;
    }

    public long getPageId() {
        return pageId;
    }

    public PageType getType() {
        return type;
    }

    public byte[] getData() {
        return Arrays.copyOf(data, data.length);
    }

    public void write(int offset, byte[] bytes) {
        Objects.checkFromIndexSize(offset, bytes.length, data.length);
        System.arraycopy(bytes, 0, data, offset, bytes.length);
        dirty = true;
    }

    public void overwrite(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != data.length) {
            throw new IllegalArgumentException("Expected " + data.length + " bytes but received " + bytes.length);
        }
        System.arraycopy(bytes, 0, data, 0, data.length);
        dirty = true;
    }

    public long getLsn() {
        return lsn;
    }

    public void setLsn(long lsn) {
        this.lsn = lsn;
        dirty = true;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void markClean() {
        dirty = false;
    }
}
