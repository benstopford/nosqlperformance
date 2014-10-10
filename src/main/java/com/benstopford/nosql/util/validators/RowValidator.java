package com.benstopford.nosql.util.validators;

public interface RowValidator {

    void validate(Object key, Object value);

    long totalBytes();

    void assertCountIsValid(long expected);

    void reset();

    long valueBytes();

    static final RowValidator NULL = new RowValidator() {
        @Override
        public void validate(Object key, Object value) {
        }

        @Override
        public long totalBytes() {
            return 0;
        }

        @Override
        public void assertCountIsValid(long expected) {
        }

        @Override
        public void reset() {
        }
        @Override
        public long valueBytes() {
            return 0;
        }

        @Override
        public void assertTotalValueBytesAreValid(long expected) {
        }
    };

    void assertTotalValueBytesAreValid(long expected);
}