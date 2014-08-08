package com.benstopford.nosql;

public interface Runner {
    void initialise() throws Exception;

    RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) throws Exception;

    RunResult readKeyValuePairs(long numberOfEntries, int entriesPerBatch) throws Exception;

    void finalise() throws Exception;
}
