package com.benstopford.nosql;

import com.benstopford.nosql.tests.RandomRead;
import com.benstopford.nosql.tests.SequentialRead;
import com.benstopford.nosql.tests.SequentialWrite;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.Result;
import com.benstopford.nosql.util.validators.CountingValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.benstopford.nosql.util.OutputUtils.*;
import static java.lang.String.format;

public class Main {
    private static Logger log = Logger.instance();

    private RunState runState;

    public static class RunState {
        public Long writesSoFar = 0L;
        public Map<String,List<Result>> allResults = new HashMap<>();
    }

    static class RunParams {
        final int numberOfEntries = 10*1024;
        final int entrySizeBytes = 1024;
        final long iterations = 100;
        final long batchSize = 1000;

        public long dataSize() {
            return iterations * numberOfEntries * entrySizeBytes;
        }
    }

    public static void main(String[] args) throws Exception {
        new Main(new RunParams());
    }

    String[] dbs = {
//            "com.benstopford.nosql.databases.couchbase.Couchbase",
            "com.benstopford.nosql.databases.coherence.Coherence",
            "com.benstopford.nosql.databases.cassandra.Cassandra"
    };

    public Main(RunParams args) throws Exception {
        runState = new RunState();
        DB db = null;
        try {
            for (String name : dbs) {
                db = (DB) Class.forName(name).newInstance();
                runState.writesSoFar = 0L;
                String className = name(db);
                log.info(format("%s - Running up to a max dataset size of %,dB\n", className, args.dataSize()));
                db.initialise();
                List<Result> results = new ArrayList();

                for (int i = 0; i < args.iterations; i++) {
                    track(results,
                            new SequentialWrite(runState, db, runState.writesSoFar, runState.writesSoFar + args.numberOfEntries, args.entrySizeBytes, args.batchSize).execute());
                    track(results,
                            new SequentialRead(runState, db, 0, args.numberOfEntries, args.entrySizeBytes, args.batchSize, results).execute());
                    track(results,
                            new RandomRead(runState, db, runState.writesSoFar - 1, args.numberOfEntries, args.entrySizeBytes, args.batchSize, new CountingValidator()).execute());

                    //save state
                    runState.allResults.put(className, results);
                    saveToFile(results, className);
                    printChart(results, className);
                }
                copyChartToDataOuptutDir(className);
            }

            printCombinedChart(runState.allResults);
            copyChartToDataOuptutDir("Combined");

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            db.clearDown();
            System.exit(-1);
        }


    }

    private String name(DB db) {
        String name = db.getClass().getSimpleName() + System.currentTimeMillis();
        log.info("Execution name:" + name);
        return name;
    }

    private void track(List<Result> state, Result execute) {
        state.add(execute);
        log.info(execute.toString());
    }
}
