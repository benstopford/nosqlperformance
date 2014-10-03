package com.benstopford.nosql.tests;

import com.benstopford.nosql.util.validators.CountingValidator;
import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.PerformanceTimer;
import com.benstopford.nosql.util.Result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.benstopford.nosql.util.PerformanceTimer.end;
import static com.benstopford.nosql.util.PerformanceTimer.start;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.valueOf;

;

public class SequentialRead implements PerformanceTest {
    Logger log = Logger.instance();

    private Main.RunContext runContext;
    private DB db;
    private long from;
    private long to;
    private long entrySize;
    private long batchSize;
    private List<Result> state;

    public SequentialRead(Main.RunContext runContext, DB db, long from, long to, long entrySize, long batchSize, List<Result> state) {
        this.runContext = runContext;
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

        Result result = new Result("ReadSeq", (to - from) * entrySize, countingValidator.valueBytes, batchSize, took.ms(), 1000000000L * countingValidator.totalBytes() / took.ns(), runContext.writesSoFar * entrySize);

        checkArgument(countingValidator.count == to - from, "Read count did not match", countingValidator, result);
        checkArgument(countingValidator.valueBytes == result.estimatedBytes, "Read bytes did not match", countingValidator, result);

        log.info(result);

        return result;

    }
}
