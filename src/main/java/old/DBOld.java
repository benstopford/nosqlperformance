package old;

import com.benstopford.nosql.util.validators.RowValidator;

import java.util.Collection;
import java.util.Map;

public interface DBOld {
    void initialise() throws Exception;

    void clearDown() throws Exception;

    RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) throws Exception;

    RunResult readKeyValuePairs(long numberOfEntries, int entriesPerBatch) throws Exception;

    RunResult readKeyValuePair(Collection<Integer> keys) throws Exception;


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

    void finalise() throws Exception;

}
