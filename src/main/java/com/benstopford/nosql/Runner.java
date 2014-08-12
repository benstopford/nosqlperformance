package com.benstopford.nosql;

import java.util.Collection;

public interface Runner {
    void initialise() throws Exception;

    void clearDown() throws Exception;

    RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) throws Exception;

    RunResult readKeyValuePairs(long numberOfEntries, int entriesPerBatch) throws Exception;

    RunResult readKeyValuePair(Collection<Integer> keys) throws Exception;


    void finalise() throws Exception;
}
