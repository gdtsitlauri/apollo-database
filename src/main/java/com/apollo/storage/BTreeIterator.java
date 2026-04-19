package com.apollo.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class BTreeIterator<K extends Comparable<K>, V> implements Iterator<Map.Entry<K, V>> {
    private final Iterator<Map.Entry<K, V>> delegate;

    public BTreeIterator(NavigableMap<K, V> values) {
        this.delegate = values.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
        return delegate.next();
    }
}
