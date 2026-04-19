package com.apollo.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeapFileTest {
    @TempDir
    Path tempDir;

    @Test
    void testHeapFileCrud() {
        PageManager pageManager = new PageManager(256);
        BufferPool bufferPool = new BufferPool(8, pageManager);
        HeapFile heapFile = new HeapFile(pageManager, bufferPool);

        RecordId id = heapFile.insert(Record.fromString("apollo"));
        assertEquals("apollo", heapFile.read(id).orElseThrow().asString());

        assertTrue(heapFile.update(id, Record.fromString("database")));
        assertEquals("database", heapFile.read(id).orElseThrow().asString());

        assertTrue(heapFile.delete(id));
        assertTrue(heapFile.read(id).isEmpty());
    }

    @Test
    void testHeapFilePersistsAcrossRestart() {
        Path backingFile = tempDir.resolve("heap-pages.bin");

        PageManager firstManager = new PageManager(512, backingFile);
        BufferPool firstPool = new BufferPool(8, firstManager);
        HeapFile firstHeap = new HeapFile(firstManager, firstPool);

        Record initial = Record.fromString("persisted-row");
        initial.setXmin(11);
        RecordId id = firstHeap.insert(initial);

        PageManager secondManager = new PageManager(512, backingFile);
        BufferPool secondPool = new BufferPool(8, secondManager);
        HeapFile secondHeap = new HeapFile(secondManager, secondPool);

        Record restored = secondHeap.read(id).orElseThrow();
        assertEquals("persisted-row", restored.asString());
        assertEquals(11, restored.getXmin());
    }
}
