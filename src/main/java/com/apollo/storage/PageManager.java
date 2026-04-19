package com.apollo.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PageManager {
    private final int pageSize;
    private final Path backingFile;
    private final AtomicLong pageIdGenerator = new AtomicLong();
    private final Map<Long, Page> diskPages = new ConcurrentHashMap<>();

    public PageManager(int pageSize) {
        this(pageSize, null);
    }

    public PageManager(int pageSize, Path backingFile) {
        this.pageSize = pageSize;
        this.backingFile = backingFile;
        loadIfPresent();
    }

    public int getPageSize() {
        return pageSize;
    }

    public Page allocate(Page.PageType type) {
        long id = pageIdGenerator.getAndIncrement();
        Page page = new Page(id, type, pageSize);
        diskPages.put(id, page);
        return page;
    }

    public Optional<Page> read(long pageId) {
        return Optional.ofNullable(diskPages.get(pageId));
    }

    public void flush(Page page) {
        diskPages.put(page.getPageId(), page);
        page.markClean();
        persist();
    }

    public Collection<Page> pages() {
        return diskPages.values();
    }

    public void flushAll() {
        for (Page page : diskPages.values()) {
            page.markClean();
        }
        persist();
    }

    private void loadIfPresent() {
        if (backingFile == null || !Files.exists(backingFile)) {
            return;
        }

        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(backingFile)))) {
            int storedPageSize = input.readInt();
            if (storedPageSize != pageSize) {
                throw new IllegalStateException("Page size mismatch: expected " + pageSize + " but found " + storedPageSize);
            }
            long nextId = input.readLong();
            int pageCount = input.readInt();
            for (int i = 0; i < pageCount; i++) {
                long pageId = input.readLong();
                Page.PageType type = Page.PageType.valueOf(input.readUTF());
                long lsn = input.readLong();
                byte[] data = input.readNBytes(pageSize);
                diskPages.put(pageId, new Page(pageId, type, data, lsn));
            }
            pageIdGenerator.set(nextId);
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to load page file " + backingFile, exception);
        }
    }

    private void persist() {
        if (backingFile == null) {
            return;
        }

        try {
            Path parent = backingFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(backingFile)))) {
                output.writeInt(pageSize);
                output.writeLong(pageIdGenerator.get());
                output.writeInt(diskPages.size());
                for (Page page : diskPages.values()) {
                    output.writeLong(page.getPageId());
                    output.writeUTF(page.getType().name());
                    output.writeLong(page.getLsn());
                    output.write(page.getData());
                }
            }
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to persist page file " + backingFile, exception);
        }
    }
}
