package com.benstopford.nosql;

import com.benstopford.nosql.old.DBOld;

public class CountingPrintingValidator extends CountingValidator implements DBOld.RowValidator  {
    @Override
    public void validate(Object key, Object value) {
        super.validate(key,value);
        System.out.println(key);
    }
}
