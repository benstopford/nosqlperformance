package com.benstopford.nosql.util;

import java.io.Serializable;

public class Result implements Serializable{
    public String name;
    public long estimatedBytes;
    public long recordedBytes;
    public long batchSize;
    public long ms;
    public long throughput;
    public long cumulativeSize;


    public Result(String name, long estimatedBytes, long recordedBytes, long batchSize, long ms, long throughput, long cumulativeSize) {
        this.name = name;
        this.estimatedBytes = estimatedBytes;
        this.recordedBytes = recordedBytes;
        this.batchSize = batchSize;
        this.ms = ms;
        this.throughput = throughput;
        this.cumulativeSize = cumulativeSize;
    }

    @Override
    public String toString() {
        return String.format("%s [iterBytes:%,dB][recordedBytes:%,dB][batch:%,d][took:%,dms]" +
                        "[throughput:%,dB/s][totalSize:%,dB]",
                name,
                estimatedBytes,
                recordedBytes,
                batchSize,
                ms,
                throughput,
                cumulativeSize
        );
    }


}
