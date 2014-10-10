package com.benstopford.nosql.tests;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.PerformanceTimer;
import com.benstopford.nosql.util.Result;
import com.benstopford.nosql.util.validators.RowValidator;
import com.google.common.base.Preconditions;

import java.util.*;

import static com.benstopford.nosql.util.PerformanceTimer.end;
import static com.benstopford.nosql.util.PerformanceTimer.start;
import static java.lang.String.valueOf;

;

public class RandomRead implements PerformanceTest {
    Logger log = Logger.instance();

    private Main.RunState runState;
    private DB db;
    private long incrementalKeySpace;
    private long readCount;
    private long entrySize;
    private long batch;
    private RowValidator validator;
    private Random random = new Random();

    public RandomRead(Main.RunState runState, DB db, long incrementalKeySpace, long readCount, long entrySize, long batch, RowValidator rowValidator) throws Exception {
        this.runState = runState;
        this.db = db;
        this.incrementalKeySpace = incrementalKeySpace;
        this.readCount = readCount;
        this.entrySize = entrySize;
        this.batch = batch;
        this.validator = rowValidator;
    }

    @Override
    public Result execute() throws Exception {
        validator.reset();

        long count = readCount;
        //appears that random reads read a batch of keys and they may not be unique .. hence less results
        start();
        while (count > 0) {
            Collection<String> keys = getKeys(count);
            db.read(keys, validator);
            count -= keys.size();
        }

        PerformanceTimer.Took took = end();

        Result result = new Result("ReadRand", readCount * entrySize, validator.valueBytes(), batch, took.ms(), 1000000000L * validator.totalBytes() / took.ns(), runState.writesSoFar * entrySize);
        validator.assertCountIsValid(readCount);
        return result;
    }

    private Collection<String> getKeys(long count) {
        Collection<String> theKeys;
        if (count > batch)
            theKeys = nRandomKeys(batch);
        else
            theKeys = nRandomKeys(count);
        return theKeys;
    }

    private Collection<String> nRandomKeys(long batch) {
        Set<String> keys = new HashSet();
        while (keys.size() < batch) {
            String key = valueOf(Math.round(random.nextDouble() * incrementalKeySpace));
            keys.add(key);
        }
        Preconditions.checkState(keys.size() == batch);
        return keys;
    }
}
