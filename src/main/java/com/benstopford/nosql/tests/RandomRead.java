package com.benstopford.nosql.tests;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.PerformanceTimer;
import com.benstopford.nosql.util.Result;
import com.benstopford.nosql.util.validators.CountingValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.benstopford.nosql.util.PerformanceTimer.end;
import static com.benstopford.nosql.util.PerformanceTimer.start;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.valueOf;

;

public class RandomRead implements PerformanceTest {
    Logger log = Logger.instance();

    private Main.RunContext runContext;
    private DB db;
    private long incrementalKeySpace;
    private long readCount;
    private long entrySize;
    private long batch;
    private List<Result> state;

    public RandomRead(Main.RunContext runContext, DB db, long incrementalKeySpace, long readCount, long entrySize, long batch, List<Result> state) throws Exception {
        this.runContext = runContext;
        this.db = db;
        this.incrementalKeySpace = incrementalKeySpace;
        this.readCount = readCount;
        this.entrySize = entrySize;
        this.batch = batch;
        this.state = state;
    }

    @Override
    public Result execute() throws Exception {
        List<String> keys = new ArrayList();
        Random random = new Random();
        CountingValidator validator = new CountingValidator();

        start();
        for (int i = 0; i < readCount; i++) {
            int key = (int) Math.round(random.nextDouble() * incrementalKeySpace);
            keys.add(valueOf(key));
            if (i % batch == 0) {
                db.read(keys, validator);
                keys.clear();
            }
        }
        if (keys.size() > 0)
            db.read(keys, validator);

        PerformanceTimer.Took took = end();

        Result result = new Result("ReadRand", readCount * entrySize, validator.valueBytes, batch, took.ms(), 1000000000L * validator.totalBytes() / took.ns(), runContext.writesSoFar * entrySize);
        checkState(validator.count == readCount, "Counts did not match %s != %s", validator.count, readCount, result);

        log.info(result.toString());
        return result;
    }
}
