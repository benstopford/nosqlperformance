package com.benstopford.nosql.util.validators;

import com.google.common.base.Preconditions;

public class CountingValidator implements RowValidator {
    private long count = 0;
    private long keyBytes = 0;
    private long valueBytes = 0;
    @Override
    public void validate(Object key, Object value) {
        try {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(value);

            count++;
            keyBytes += ((String) key).getBytes().length;
            valueBytes += ((String) value).getBytes().length;
        }
        catch (Exception e){
            System.err.printf("Failed for read %s %s. Current stats are count:%s, keyBytes:%s, valueBytes:%s\n", key, value, count, keyBytes, valueBytes);
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public long totalBytes(){
        return keyBytes+valueBytes;
    }

    @Override
    public String toString() {
        return "CountingValidator{" +
                "count=" + count +
                ", keyBytes=" + keyBytes +
                ", valueBytes=" + valueBytes +
                ", totalBytes=" + totalBytes() +
                '}';
    }

    @Override
    public void assertCountIsValid(long expected) {
        Preconditions.checkState(count==expected, "Counts did not match %s != %s", count, expected, this);
    }

    @Override
    public void reset() {
        count = 0;
        keyBytes = 0;
        valueBytes = 0;
    }

    @Override
    public long valueBytes() {
        return valueBytes;
    }

    @Override
    public void assertTotalValueBytesAreValid(long expected) {
        Preconditions.checkState(valueBytes==expected, "Bytes did not match %s != %s", count, expected, this);
    }
}
