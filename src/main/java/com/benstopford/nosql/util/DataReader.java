package com.benstopford.nosql.util;

import com.thoughtworks.xstream.XStream;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DataReader {
    public static void main(String[] args) throws Exception {
        List<Result> data  = (List<Result>) new XStream()
                .fromXML(new String(Files.readAllBytes(Paths.get("data/Couchbase1411734654787.xml"))));


//        Main.printChart(data, db);
        OutputUtils.copyChartToDataOuptutDir("Couchbase1411734654787");
    }
}
