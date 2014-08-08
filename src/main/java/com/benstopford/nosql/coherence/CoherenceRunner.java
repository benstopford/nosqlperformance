package com.benstopford.nosql.coherence;

import com.benstopford.nosql.RunResult;
import com.benstopford.nosql.Runner;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoherenceRunner implements Runner {
    public static final Integer PORT = 34189;
    private NamedCache cache;

    public void initialise() throws Exception {
        System.setProperty("client.extend.port", PORT.toString());
        cache = CacheFactory.getCacheFactoryBuilder()
                .getConfigurableCacheFactory("config/client.xml", ClassLoader.getSystemClassLoader())
                .ensureCache("test", ClassLoader.getSystemClassLoader());
        cache.clear();
        System.out.println("Connected...");
    }

    public RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) {
        byte[] value = new byte[entrySizeBytes];
        Map batch = new HashMap();
        long totalBytesWritten = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfEntries; i++) {
            batch.put(i, value);
            if (i % entriesPerBatch == 0 || i==numberOfEntries-1) {
                totalBytesWritten = writeBatch(value, batch, totalBytesWritten);
            }
        }
        totalBytesWritten = writeBatch(value, batch, totalBytesWritten);

        long took = System.currentTimeMillis() - start;


        RunResult runResult = new RunResult("Load Coherence");
        runResult.populate(totalBytesWritten, took, entrySizeBytes, numberOfEntries, entriesPerBatch);
        return runResult;
    }

    private long writeBatch(byte[] value, Map batch, long totalBytesWritten) {
        cache.putAll(batch);
        totalBytesWritten = totalBytesWritten + (batch.size() * value.length);
        batch.clear();
        return totalBytesWritten;
    }

    public RunResult readKeyValuePairs(long numberOfEntries, int entriesPerBatch) {
        long totalBytesRead = 0;
        List batch = new ArrayList();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfEntries; i++) {
            batch.add(i);
            if (i % entriesPerBatch == 0 || i==numberOfEntries-1) {
                totalBytesRead = readBatch(totalBytesRead, batch);
            }
        }

        long took = System.currentTimeMillis() - start;
        RunResult runResult = new RunResult("Read Coherence");
        runResult.populate(totalBytesRead, took, -1, numberOfEntries, entriesPerBatch);
        return runResult;
    }

    private long readBatch(long totalBytesRead, List batch) {
        Map all = cache.getAll(batch);
        for (Object o : all.values()) {
            byte[] data = (byte[]) o;
            totalBytesRead += data.length;
        }
        batch.clear();
        return totalBytesRead;
    }

    public void finalise() {
        cache.destroy();
    }
}
