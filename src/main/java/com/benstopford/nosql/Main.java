package com.benstopford.nosql;

import com.benstopford.nosql.tests.RandomRead;
import com.benstopford.nosql.tests.SequentialRead;
import com.benstopford.nosql.tests.SequentialWrite;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.OutputUtils;
import com.benstopford.nosql.util.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.benstopford.nosql.util.OutputUtils.Series;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Main {
    private static Logger log = Logger.instance();

    private RunContext runContext;

    public static class RunContext {
        public Long writesSoFar = 0L;
    }

    static class RunParams {
        final int numberOfEntries = 2 * 1024;
        final int entrySizeBytes = 1024;
        final long iterations = 4;
        final long batchSize = 100;

        public long dataSize() {
            return iterations * numberOfEntries * entrySizeBytes;
        }
    }

    public static void main(String[] args) throws Exception {
        new Main(new RunParams());
    }

    public Main(RunParams args) throws Exception {
        DB db = (DB) Class.forName("com.benstopford.nosql.databases.couchbase.Couchbase").newInstance();
        try {
            go(name(db), db, args);
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

    private void go(String name, DB db, RunParams p) throws Exception {
        log.info(format("Running up to a max dataset size of %,dB\n", p.dataSize()));
        db.initialise();
        runContext = new RunContext();
        List<Result> state = new ArrayList();

        for (int i = 0; i < p.iterations; i++) {
            process(state,
                    new SequentialWrite(runContext, db, runContext.writesSoFar, runContext.writesSoFar + p.numberOfEntries, p.entrySizeBytes, p.batchSize).execute());
            process(state,
                    new SequentialRead(runContext, db, 0, p.numberOfEntries, p.entrySizeBytes, p.batchSize, state).execute());
            process(state,
                    new RandomRead(runContext, db, runContext.writesSoFar - 1, p.numberOfEntries, p.entrySizeBytes, p.batchSize, state).execute());

            OutputUtils.save(state, name);
            printChart(state, db);
        }

        OutputUtils.copyChartToDataOuptutDir(name);
    }

    private void process(List<Result> state, Result execute) {
        state.add(execute);
        log.info(execute.toString());
    }

    public void printChart(List<Result> data, DB db) throws Exception {
        String dbName = db.getClass().getSimpleName();

        Function<String, List<Long>> throughputFilter = (String type) -> data.stream()
                .filter(result -> type.equals(result.name))
                .map(result -> result.throughput)
                .collect(toList());

        OutputUtils.seriesChart(
                Series.of(dbName, "ReadRand", throughputFilter.apply("ReadRand")),
                Series.of(dbName, "ReadSeq", throughputFilter.apply("ReadSeq")),
                Series.of(dbName, "Write", throughputFilter.apply("Write"))
        );
    }
}
