package com.apollo.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class BufferPool {
    private final int capacity;
    private final PageManager pageManager;
    private final LinkedHashMap<Long, Page> cache;

    public BufferPool(int capacity, PageManager pageManager) {
        this.capacity = capacity;
        this.pageManager = Objects.requireNonNull(pageManager, "pageManager");
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true);
    }

    public synchronized Page getPage(long pageId) {
        Page cached = cache.get(pageId);
        if (cached != null) {
            return cached;
        }

        Page page = pageManager.read(pageId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown page " + pageId));
        evictIfNecessary();
        cache.put(pageId, page);
        return page;
    }

    public synchronized void putPage(Page page) {
        evictIfNecessary();
        cache.put(page.getPageId(), page);
    }

    public synchronized Optional<Page> peek(long pageId) {
        return Optional.ofNullable(cache.get(pageId));
    }

    public synchronized void flushAll() {
        for (Map.Entry<Long, Page> entry : cache.entrySet()) {
            pageManager.flush(entry.getValue());
        }
    }

    public synchronized int size() {
        return cache.size();
    }

    private void evictIfNecessary() {
        if (cache.size() < capacity) {
            return;
        }

        Long eldestKey = cache.keySet().iterator().next();
        Page eldest = cache.remove(eldestKey);
        if (eldest != null && eldest.isDirty()) {
            pageManager.flush(eldest);
        }
    }
}
