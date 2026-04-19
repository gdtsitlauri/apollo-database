package com.apollo.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class Record {
    private final byte[] data;
    private long xmin;
    private Long xmax;

    public Record(byte[] data) {
        this.data = Arrays.copyOf(Objects.requireNonNull(data, "data"), data.length);
    }

    public static Record fromString(String value) {
        return new Record(value.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] getData() {
        return Arrays.copyOf(data, data.length);
    }

    public String asString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public int length() {
        return data.length;
    }

    public long getXmin() {
        return xmin;
    }

    public void setXmin(long xmin) {
        this.xmin = xmin;
    }

    public Long getXmax() {
        return xmax;
    }

    public void setXmax(Long xmax) {
        this.xmax = xmax;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Integer.BYTES + data.length);
        buffer.putLong(xmin);
        buffer.putLong(xmax == null ? Long.MIN_VALUE : xmax);
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }

    public static Record deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long xmin = buffer.getLong();
        long encodedXmax = buffer.getLong();
        int length = buffer.getInt();
        byte[] data = new byte[length];
        buffer.get(data);
        Record record = new Record(data);
        record.setXmin(xmin);
        record.setXmax(encodedXmax == Long.MIN_VALUE ? null : encodedXmax);
        return record;
    }
}
