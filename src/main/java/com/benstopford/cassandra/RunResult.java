package com.benstopford.cassandra;

public class RunResult {
    private long totalBytes;
    private long totalTime;
    private int objectSize;
    private long count;
    private long batch;
    private String name;

    public long totalTime() {
        return totalTime;
    }

    public long totalBytes() {
        return totalBytes;
    }

    public RunResult(String name) {
        this.name = name;
    }

    public void populate(long totalBytes, long totalTime, int objectSize, long count, long batch) {
        this.totalBytes = totalBytes;
        this.totalTime = totalTime;
        this.objectSize = objectSize;
        this.count = count;
        this.batch = batch;
    }

    @Override
    public String toString() {
        return name + "{" +
                "totalBytes=" + totalBytes +
                ", totalTime=" + totalTime +
                ", objectSize=" + objectSize +
                ", count=" + count +
                ", batch=" + batch +
                '}';
    }
}
