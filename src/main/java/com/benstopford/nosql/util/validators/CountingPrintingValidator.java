package com.benstopford.nosql.util.validators;


import java.util.logging.Logger;

public class CountingPrintingValidator extends CountingValidator implements RowValidator {
    Logger log = Logger.getAnonymousLogger();
    @Override
    public void validate(Object key, Object value) {
        super.validate(key,value);
        log.info(key.toString());
    }
}
