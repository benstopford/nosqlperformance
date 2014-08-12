package com.benstopford.nosql;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RunMe {
    public static void main(String[] args) throws Exception {
        new RunMe();
    }

    public RunMe() throws Exception {
        runUpDownTest();
        randomLargeDatasetReadTest();
    }

    private void runUpDownTest() throws Exception {
        List<RunResult> results = new ArrayList<RunResult>();

        writeThenReadAll(100 * 1024, 1024, 100, "com.benstopford.nosql.coherence.CoherenceRunner", results);
        writeThenReadAll(100 * 1024, 1024, 100, "com.benstopford.nosql.cassandra.CassandraRunner", results);

        writeThenReadAll(100 * 1024, 1024, 10, "com.benstopford.nosql.coherence.CoherenceRunner", results);
        writeThenReadAll(100 * 1024, 1024, 10, "com.benstopford.nosql.cassandra.CassandraRunner", results);

        writeThenReadAll(100 * 1024, 1024, 1, "com.benstopford.nosql.coherence.CoherenceRunner", results);
        writeThenReadAll(100 * 1024, 1024, 1, "com.benstopford.nosql.cassandra.CassandraRunner", results);

        System.out.println("-----------------------");
        for (RunResult r : results) {
            System.out.println(r);
        }
    }

    private void randomLargeDatasetReadTest() throws Exception {
        List<RunResult> results = new ArrayList<RunResult>();

        writeThenReadSetAtRandom(1024 * 1024 * 1024, 5 * 1024, 1000, "com.benstopford.nosql.cassandra.CassandraRunner", results, 100);
        writeThenReadSetAtRandom(1024 * 1024 * 1024, 5 * 1024, 1000, "com.benstopford.nosql.coherence.CoherenceRunner", results, 100);

        System.out.println("-----------------------");
        for (RunResult r : results) {
            System.out.println(r);
        }
    }

    private void writeThenReadAll(int numberOfEntries, int entrySizeBytes, int batch, String className, List<RunResult> results) throws Exception {
        System.out.println("Start---------------" + className + "----------------");
        System.out.printf("Data size: %,dB\n", entrySizeBytes * numberOfEntries);

        Runner run = (Runner) Class.forName(className).newInstance();
        run.initialise();
        run.clearDown();

        RunResult writeResult = run.loadKeyValuePairs(numberOfEntries, entrySizeBytes, batch);
        RunResult readResult = run.readKeyValuePairs(numberOfEntries, batch);

        run.finalise();

        check(numberOfEntries, entrySizeBytes, writeResult, readResult);

        System.out.println("End---------------" + className + "----------------");
        results.add(writeResult);
        results.add(readResult);
    }

    private void writeThenReadSetAtRandom(int numberOfEntries, int entrySizeBytes, int batch, String className, List<RunResult> results, int numKeysToRead) throws Exception {
        System.out.println("Start---------------" + className + "----------------");
        System.out.printf("Data size: %,dB\n", entrySizeBytes * numberOfEntries);

        Runner run = (Runner) Class.forName(className).newInstance();
        run.initialise();
        run.clearDown();

        RunResult writeResult = run.loadKeyValuePairs(numberOfEntries, entrySizeBytes, batch);

        List<Integer> keys = new ArrayList<Integer>();
        Random random = new Random();
        for (int i = 0; i < numKeysToRead; i++) {
            int key = (int) Math.round(random.nextDouble() * numberOfEntries);
            if (i % 1000 == 0)
                System.out.println("requesting key " + key);
            keys.add(key);
        }
        RunResult result = run.readKeyValuePair(keys);
        results.add(result);

        run.finalise();

        System.out.println("End---------------" + className + "----------------");
        results.add(writeResult);
    }

    private void check(int numberOfEntries, int entrySizeBytes, RunResult writeResult, RunResult readResult) {
        System.out.println("-------------------------------------------------");
        if (numberOfEntries * entrySizeBytes != readResult.totalBytes()) {
            System.out.println("Warning: did not read the expected data size");
            System.out.println("Expected " + numberOfEntries * entrySizeBytes);
            System.out.println("Read " + readResult.totalBytes());
            throw new RuntimeException("read/write byte count did not match");
        }
    }
}
