package old;

import com.benstopford.nosql.util.validators.RowValidator;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

import java.util.*;

public class CoherenceRunner implements DBOld {

    //Config
    public static final Integer PORT = 34344;
    public static final String ADDRESS = "localhost";

    private NamedCache cache;

    public void initialise() throws Exception {
        System.setProperty("client.extend.port", PORT.toString());
        System.setProperty("client.extend.address", ADDRESS.toString());
        cache = CacheFactory.getCacheFactoryBuilder()
                .getConfigurableCacheFactory("config/client.xml", ClassLoader.getSystemClassLoader())
                .ensureCache("test", ClassLoader.getSystemClassLoader());
        System.out.println("Connected...");
    }

    @Override
    public void clearDown() throws Exception {
        cache.clear();
    }

    public RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) {
        byte[] value = new byte[entrySizeBytes];
        Map batch = new HashMap();
        long totalBytesWritten = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfEntries; i++) {
            batch.put(i, value);
            if (i % entriesPerBatch == 0 || i == numberOfEntries - 1) {
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
        System.out.println(totalBytesWritten);
        batch.clear();
        return totalBytesWritten;
    }

    public RunResult readKeyValuePairs(long numberOfEntries, int entriesPerBatch) {
        long totalBytesRead = 0;
        List batch = new ArrayList();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfEntries; i++) {
            batch.add(i);
            if (i % entriesPerBatch == 0 || i == numberOfEntries - 1) {
                totalBytesRead = readBatch(totalBytesRead, batch);
            }
        }

        long took = System.currentTimeMillis() - start;
        RunResult runResult = new RunResult("Read Coherence");
        runResult.populate(totalBytesRead, took, -1, numberOfEntries, entriesPerBatch);
        return runResult;
    }

    @Override
    public RunResult readKeyValuePair(Collection<Integer> keys) throws Exception {
        long start = System.currentTimeMillis();

        for (Integer key : keys) {
            Object value = cache.get(key);
            if (value == null) {
                throw new RuntimeException("oops");
            }
        }

        long took = System.currentTimeMillis() - start;
        RunResult runResult = new RunResult("Read Coherence");
        runResult.populate(-1, took, -1, 1, 1);
        return runResult;
    }

    @Override
    public void load(Map<String, String> batch) {

    }


    @Override
    public void read(Collection keys, RowValidator rowValidator) {

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
