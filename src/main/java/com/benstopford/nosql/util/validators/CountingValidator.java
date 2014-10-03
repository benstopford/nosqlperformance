package com.benstopford.nosql.util.validators;

import com.google.common.base.Preconditions;

public class CountingValidator implements RowValidator {
    public long count = 0;
    public long keyBytes = 0;
    public long valueBytes = 0;
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
}
