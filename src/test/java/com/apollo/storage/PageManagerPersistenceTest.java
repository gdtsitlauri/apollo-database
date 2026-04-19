package com.apollo.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PageManagerPersistenceTest {
    @TempDir
    Path tempDir;

    @Test
    void testPagesPersistAcrossRestart() {
        Path backingFile = tempDir.resolve("pages.bin");

        PageManager writer = new PageManager(128, backingFile);
        Page page = writer.allocate(Page.PageType.DATA);
        page.write(0, "apollo".getBytes(StandardCharsets.UTF_8));
        page.setLsn(7);
        writer.flush(page);

        PageManager reader = new PageManager(128, backingFile);
        Page restored = reader.read(page.getPageId()).orElseThrow();

        byte[] expected = new byte[128];
        System.arraycopy("apollo".getBytes(StandardCharsets.UTF_8), 0, expected, 0, "apollo".length());
        assertArrayEquals(expected, restored.getData());
        assertEquals(7, restored.getLsn());
        assertEquals(Page.PageType.DATA, restored.getType());
    }
}
