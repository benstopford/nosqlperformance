package com.benstopford.nosql.tests;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.PerformanceTimer;
import com.benstopford.nosql.util.Result;
import com.benstopford.nosql.util.validators.CountingValidator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.benstopford.nosql.util.PerformanceTimer.end;
import static com.benstopford.nosql.util.PerformanceTimer.start;
import static java.lang.String.valueOf;

;

public class SequentialRead implements PerformanceTest {
    Logger log = Logger.instance();

    private Main.RunState runState;
    private DB db;
    private long from;
    private long to;
    private long entrySize;
    private long batchSize;
    private List<Result> state;

    public SequentialRead(Main.RunState runState, DB db, long from, long to, long entrySize, long batchSize, List<Result> state) {
        this.runState = runState;
        this.db = db;
        this.from = from;
        this.to = to;
        this.entrySize = entrySize;
        this.batchSize = batchSize;
        this.state = state;
    }

    @Override
    public Result execute() throws Exception {
        Collection<String> readBatch = new ArrayList<String>();
        CountingValidator countingValidator = new CountingValidator();

        start();
        for (long i = from; i < to; i++) {
            readBatch.add(valueOf(i));
            if (i % batchSize == 0) {
                db.read(readBatch, countingValidator);
                readBatch.clear();
            }
        }
        if (readBatch.size() > 0)
            db.read(readBatch, countingValidator);

        PerformanceTimer.Took took = end();

        Result result = new Result("ReadSeq", (to - from) * entrySize, countingValidator.valueBytes(), batchSize, took.ms(), 1000000000L * countingValidator.totalBytes() / took.ns(), runState.writesSoFar * entrySize);

        countingValidator.assertCountIsValid(to - from);
        countingValidator.assertTotalValueBytesAreValid(result.estimatedBytes);

        return result;

    }
}
