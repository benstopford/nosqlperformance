package com.benstopford.nosql.tests;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.Main;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.PerformanceTimer;
import com.benstopford.nosql.util.Result;

import java.util.HashMap;
import java.util.Map;

import static com.benstopford.nosql.util.PerformanceTimer.end;
import static com.benstopford.nosql.util.PerformanceTimer.start;
import static java.lang.String.valueOf;

public class SequentialWrite implements PerformanceTest {
    private static final Logger log = Logger.instance();
    private Main.RunContext c;
    private DB db;
    private long from;
    private long to;
    private int entrySize;
    private long batchSize;

    public SequentialWrite(Main.RunContext context, DB db, long from, long to, int entrySize, long batchSize) {
        this.c = context;
        this.db = db;
        this.from = from;
        this.to = to;
        this.entrySize = entrySize;
        this.batchSize = batchSize;
    }

    public Result execute() throws Exception {
        long bytesWritten = 0;
        String data = new String(new char[entrySize]);
        Map<String, String> writeBatch = new HashMap();

        log.info("Inserting ids %s to %s", from, to);

        long from = this.from;
        long to = this.to;

        start();
        for (; from < to; from++) {
            writeBatch.put(valueOf(from), data);
            if (from % batchSize == 0 || from == to - 1) {
                db.load(writeBatch);
                bytesWritten += writeBatch.values().stream().mapToLong(s -> s.toCharArray().length).sum();
                c.writesSoFar+= writeBatch.size();
                writeBatch.clear();
            }
        }
        PerformanceTimer.Took took = end();

        return new Result(
                "Write",
                (this.to - this.from) * entrySize,
                bytesWritten,
                batchSize,
                took.ms(),
                1000000000L * bytesWritten / took.ns(),
                c.writesSoFar * entrySize);
    }
}
