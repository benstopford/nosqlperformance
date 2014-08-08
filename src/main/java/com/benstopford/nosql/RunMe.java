package com.benstopford.nosql;

import java.util.ArrayList;
import java.util.List;

public class RunMe {
    public static void main(String[] args) throws Exception {
        new RunMe();
    }

    public RunMe() throws Exception {

        List<RunResult> results = new ArrayList<RunResult>();

        run(1024 * 1024, 1024, 1000, "com.benstopford.cassandra.CassandraRunner", results);
        run(1024 * 1024, 1024, 1000, "com.benstopford.cassandra.CoherenceRunner", results);

        run(1024 * 1024, 1024, 100, "com.benstopford.cassandra.CassandraRunner", results);
        run(1024 * 1024, 1024, 100, "com.benstopford.cassandra.CoherenceRunner", results);

        run(1024, 1024, 1000, "com.benstopford.cassandra.CassandraRunner", results);
        run(1024, 1024, 1000, "com.benstopford.cassandra.CoherenceRunner", results);

        run(1024, 1024, 100, "com.benstopford.cassandra.CassandraRunner", results);
        run(1024, 1024, 100, "com.benstopford.cassandra.CoherenceRunner", results);

        System.out.println("-----------------------");
        for (RunResult r : results) {
            System.out.println(r);
        }

    }

    private void run(int numberOfEntries, int entrySizeBytes, int batch, String className, List<RunResult> results) throws Exception {
        System.out.println("Start---------------" + className + "----------------");
        System.out.printf("Datasixe: %,dB\n", entrySizeBytes * numberOfEntries);

        Runner run = (Runner) Class.forName(className).newInstance();
        run.initialise();

        RunResult writeResult = run.loadKeyValuePairs(numberOfEntries, entrySizeBytes, batch);
        RunResult readResult = run.readKeyValuePairs(numberOfEntries, batch);

        run.finalise();

        check(numberOfEntries, entrySizeBytes, writeResult, readResult);

        System.out.println("End---------------" + className + "----------------");
        results.add(writeResult);
        results.add(readResult);
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
