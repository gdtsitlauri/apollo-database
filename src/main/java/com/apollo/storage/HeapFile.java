package com.apollo.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class HeapFile {
    private static final int SLOT_HEADER_BYTES = Integer.BYTES;
    private final PageManager pageManager;
    private final BufferPool bufferPool;
    private final int pageSize;
    private final Map<Long, List<Record>> pageRecords = new LinkedHashMap<>();

    public HeapFile(PageManager pageManager, BufferPool bufferPool) {
        this.pageManager = Objects.requireNonNull(pageManager, "pageManager");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.pageSize = pageManager.getPageSize();
        loadExistingPages();
    }

    public synchronized RecordId insert(Record record) {
        Objects.requireNonNull(record, "record");
        long targetPageId = findPageWithSpace(record.length()).orElseGet(this::allocateDataPage);
        List<Record> records = pageRecords.computeIfAbsent(targetPageId, ignored -> new ArrayList<>());
        int slotId = firstReusableSlot(records).orElse(records.size());
        if (slotId == records.size()) {
            records.add(record);
        } else {
            records.set(slotId, record);
        }
        persistPage(targetPageId, records);
        return new RecordId(targetPageId, slotId);
    }

    public synchronized Optional<Record> read(RecordId recordId) {
        List<Record> records = pageRecords.get(recordId.pageId());
        if (records == null || recordId.slotId() >= records.size()) {
            return Optional.empty();
        }
        return Optional.ofNullable(records.get(recordId.slotId()));
    }

    public synchronized boolean delete(RecordId recordId) {
        List<Record> records = pageRecords.get(recordId.pageId());
        if (records == null || recordId.slotId() >= records.size() || records.get(recordId.slotId()) == null) {
            return false;
        }
        records.set(recordId.slotId(), null);
        persistPage(recordId.pageId(), records);
        return true;
    }

    public synchronized boolean update(RecordId recordId, Record updated) {
        Objects.requireNonNull(updated, "updated");
        List<Record> records = pageRecords.get(recordId.pageId());
        if (records == null || recordId.slotId() >= records.size() || records.get(recordId.slotId()) == null) {
            return false;
        }
        records.set(recordId.slotId(), updated);
        persistPage(recordId.pageId(), records);
        return true;
    }

    public synchronized List<Record> scan() {
        List<Record> rows = new ArrayList<>();
        for (List<Record> records : pageRecords.values()) {
            for (Record record : records) {
                if (record != null) {
                    rows.add(record);
                }
            }
        }
        return rows;
    }

    public synchronized List<RecordEntry> scanEntries() {
        List<RecordEntry> entries = new ArrayList<>();
        for (Map.Entry<Long, List<Record>> pageEntry : pageRecords.entrySet()) {
            List<Record> records = pageEntry.getValue();
            for (int slotId = 0; slotId < records.size(); slotId++) {
                Record record = records.get(slotId);
                if (record != null) {
                    entries.add(new RecordEntry(new RecordId(pageEntry.getKey(), slotId), record));
                }
            }
        }
        return entries;
    }

    private Optional<Long> findPageWithSpace(int recordLength) {
        return pageRecords.entrySet().stream()
                .filter(entry -> canFit(entry.getValue(), recordLength))
                .map(Map.Entry::getKey)
                .findFirst();
    }

    private long allocateDataPage() {
        Page page = pageManager.allocate(Page.PageType.DATA);
        bufferPool.putPage(page);
        pageRecords.put(page.getPageId(), new ArrayList<>());
        persistPage(page.getPageId(), pageRecords.get(page.getPageId()));
        return page.getPageId();
    }

    private int usedBytes(List<Record> records) {
        int total = Integer.BYTES;
        for (Record record : records) {
            total += SLOT_HEADER_BYTES;
            if (record != null) {
                total += record.serialize().length;
            }
        }
        return total;
    }

    private boolean canFit(List<Record> records, int recordLength) {
        int recordBytes = serializedRecordBytes(recordLength);
        if (records.stream().anyMatch(Objects::isNull)) {
            return usedBytes(records) + recordBytes <= pageSize;
        }
        return usedBytes(records) + SLOT_HEADER_BYTES + recordBytes <= pageSize;
    }

    private Optional<Integer> firstReusableSlot(List<Record> records) {
        for (int i = 0; i < records.size(); i++) {
            if (records.get(i) == null) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    private void persistPage(long pageId, List<Record> records) {
        Page page = bufferPool.getPage(pageId);
        page.overwrite(serializePage(records));
        page.setLsn(page.getLsn() + 1);
        pageManager.flush(page);
    }

    private byte[] serializePage(List<Record> records) {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        buffer.putInt(records.size());
        for (Record record : records) {
            if (record == null) {
                buffer.putInt(-1);
                continue;
            }
            byte[] serialized = record.serialize();
            buffer.putInt(serialized.length);
            buffer.put(serialized);
        }
        return buffer.array();
    }

    private List<Record> deserializePage(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        List<Record> records = new ArrayList<>();
        if (bytes.length < Integer.BYTES) {
            return records;
        }
        int slots = buffer.getInt();
        for (int i = 0; i < slots; i++) {
            if (buffer.remaining() < Integer.BYTES) {
                break;
            }
            int length = buffer.getInt();
            if (length < 0) {
                records.add(null);
                continue;
            }
            byte[] recordBytes = new byte[length];
            buffer.get(recordBytes);
            records.add(Record.deserialize(recordBytes));
        }
        return records;
    }

    private int serializedRecordBytes(int recordLength) {
        return Long.BYTES + Long.BYTES + Integer.BYTES + recordLength;
    }

    private void loadExistingPages() {
        pageManager.pages().stream()
                .filter(page -> page.getType() == Page.PageType.DATA)
                .sorted(Comparator.comparingLong(Page::getPageId))
                .forEach(page -> {
                    bufferPool.putPage(page);
                    pageRecords.put(page.getPageId(), deserializePage(page.getData()));
                });
    }

    public record RecordEntry(RecordId recordId, Record record) {
    }
}
