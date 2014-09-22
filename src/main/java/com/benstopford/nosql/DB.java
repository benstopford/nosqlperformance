package com.benstopford.nosql;

import com.benstopford.nosql.old.RunResult;

import java.util.Collection;
import java.util.Map;

public interface DB {
    void initialise() throws Exception;

    void clearDown() throws Exception;

    void finalise() throws Exception;

    /**
     * The Database is required to load the supplied set of key value pairs into the database.
     * @param batch - the key-value pairs to load.
     * @return
     */
    void load(Map<String, String> batch);


    /**
     * Database is is required to read the supplied keys and pass each resulting key-value pair to the validator
     * @param keys - the keys to write
     * @param validator - the validator used to validate each key-value pair in the result
     */
    void read(Collection<String> keys, RowValidator rowValidator);


    /**
     * Callback interface used to validate data as it is returned to the test suite
     */
    interface RowValidator{
        static final RowValidator NULL = new RowValidator() {
            @Override
            public void validate(Object key, Object value) {

            }
        };

        void validate(Object key, Object value);
    }
}
