package com.benstopford.nosql;

public class CountingValidator implements DB.RowValidator {
    long count = 0;
    long keyBytes = 0;
    long valueBytes = 0;
    @Override
    public void validate(Object key, Object value) {
        count++;
        keyBytes+= ((String)key).getBytes().length;
        valueBytes+= ((String)value).getBytes().length;
    }

    public long totalBytes(){
        return keyBytes+valueBytes;
    }
}
