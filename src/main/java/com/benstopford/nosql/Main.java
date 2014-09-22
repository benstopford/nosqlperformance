package com.benstopford.nosql;

import java.util.*;

import static com.benstopford.nosql.util.PerformanceTimer.*;

public class Main {

    int cumulativeWriteCount = 0;

    public static void main(String[] args) throws Exception {
        new Main();
    }

    public Main() throws Exception {
        go();
    }

    private void go() throws Exception {
        DB db = (DB) Class.forName("com.benstopford.nosql.cassandra.Cassandra").newInstance();
        db.initialise();

        try {
            runTests(db);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.clearDown();
            System.exit(-1);
        }

    }

    private void runTests(DB db) throws Exception {
        int numberOfEntries = 100 * 1024;
        int entrySizeBytes = 1024;
        long iterations = 200;

        System.out.printf("Running up to a max dataset size of %,dB\n", iterations * numberOfEntries * entrySizeBytes);

        for (int i = 0; i < iterations; i++) {
            write(db, numberOfEntries, entrySizeBytes, 100);
            readRandom(db, cumulativeWriteCount, numberOfEntries, entrySizeBytes, 100);
            readSequential(db, numberOfEntries, entrySizeBytes, 100);
        }
    }


    private void write(DB db, int numberOfEntries, int entrySizeBytes, int batchSize) throws Exception {

        String value = new String(new char[1024]);

        Map<String, String> writeBatch = new HashMap<String, String>();

        start();
        for (int i = 0; i < numberOfEntries; i++) {
            writeBatch.put(String.valueOf(i), value);
            if (i % batchSize == 0 || i == numberOfEntries - 1) {
                db.load(writeBatch);
                cumulativeWriteCount += writeBatch.size();
                writeBatch.clear();
            }
        }
        Took took = end();

        log("Write:", numberOfEntries, entrySizeBytes, batchSize, took, -1);

    }


    private void readSequential(DB db, long readCount, long entrySize, long batchSize) throws Exception {

        Collection<String> readBatch = new ArrayList<String>();
        CountingValidator countingValidator = new CountingValidator();

        start();
        for (int i = 0; i < readCount; i++) {
            readBatch.add(String.valueOf(i));
            if (i % batchSize == 0) {
                db.read(readBatch, countingValidator);
                readBatch.clear();
            }
        }
        if (readBatch.size() > 0)
            db.read(readBatch, countingValidator);

        Took took = end();

        assertEqual(countingValidator.count, readCount);

        log("ReadSeq:", readCount, entrySize, batchSize, took, countingValidator.totalBytes());

    }

    private void log(String type, long readCount, long entrySize, long batchSize, Took took, long recordedBytes) {
        System.out.printf("%s [input:%,dB][output:%,dB][batch:%,d][took:%,dms][Throughput:%,dB/s][totalSize:%,dB]\n", type,
                readCount * entrySize, recordedBytes, batchSize, took.ms(), (readCount * entrySize * 1000L) / took.ms()
                , cumulativeWriteCount * entrySize);
    }

    private void assertEqual(Object a, Object b) {
        if (!a.equals(b)) throw new RuntimeException(a + " is not " + b);
    }


    private void readRandom(DB db, long incrementalKeySpace, long readCount, long entrySize, long batch) throws Exception {

        List<String> keys = new ArrayList<String>();
        Random random = new Random();
        CountingValidator countingValidator = new CountingValidator();

        start();
        for (int i = 0; i < readCount; i++) {
            int key = (int) Math.round(random.nextDouble() * incrementalKeySpace);
            keys.add(String.valueOf(key));
            if (i % batch == 0) {
                db.read(keys, countingValidator);
                keys.clear();
            }
        }
        if (keys.size() > 0)
            db.read(keys, countingValidator);

        Took took = end();

        //todo renable me
        // assertEqual(countingValidator.count, readCount);

        log("ReadRand:", readCount, entrySize, batch, took, countingValidator.totalBytes());
    }
}
