package com.benstopford.nosql.util.validators;

public interface RowValidator {

    static final RowValidator NULL = new RowValidator() {
        @Override
        public void validate(Object key, Object value) {

        }
    };

    void validate(Object key, Object value);
}