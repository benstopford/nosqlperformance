package com.benstopford.nosql.util;

import com.thoughtworks.xstream.XStream;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class OutputUtils {
    private static Logger log = Logger.instance();

    public static final String data = "data.js";
    public static final String template = "data.js.template";
    public static final String baseDir = "src/main/charts/";
    public static final String latestDir = baseDir + "latest/";
    public static final File libDir = new File(baseDir + "lib/");
    public static final String chartsHtml = "chart.html";
    public static final Path chartDataLocation = Paths.get(latestDir + data);
    public static final Path templateLocation = Paths.get(latestDir + template);
    public static final Path chartLocation = Paths.get(latestDir + chartsHtml);
    public static final String dataOutputDirName = "data/";
    public static final File dataOutputDir = new File(dataOutputDirName);
    public static final File libOutputDir = new File(dataOutputDirName + "lib");

    public static void seriesChart(Series... theSeries) throws Exception {
        for (Series s : theSeries)
            log.info(s.toString());

        String template = new String(Files.readAllBytes(templateLocation));

        StringBuffer data = new StringBuffer();
        for (Series s : theSeries) {
            List x = new ArrayList();
            x.add("\"" + s.testName + "\"");
            x.addAll(s.series);
            data.append(x);
            data.append(",");
        }
        String dataJs = template.replace("#data#", data.toString());

        Files.write(chartDataLocation, dataJs.getBytes());

    }

    public static void copyChartToDataOuptutDir(String dirname) throws IOException {
        File dir = new File(dataOutputDir, dirname);
        if (!dir.exists()) {
            dir.mkdir();
        }
        FileUtils.copyDirectory(new File(latestDir), dir);

        FileUtils.copyDirectory(libDir, libOutputDir);
    }

    public static void save(List<Result> state, String name) throws IOException {
        String s = new XStream().toXML(state);
        File toDir = new File(dataOutputDir, name);
        if (!toDir.exists())
            toDir.mkdir();

        String file = toDir.getAbsolutePath() + "/" + name + ".xml";
        Files.write(Paths.get(file), s.getBytes());
    }

    public static class Series {
        private String dbName;
        String testName;
        List<Long> series;

        public static Series of(String dbName, String testName, List<Long> series) {
            return new Series(dbName, testName, series);
        }

        private Series(String dbName, String testName, List<Long> series) {
            this.dbName = dbName;
            this.testName = testName;
            this.series = series;
        }

        @Override
        public String toString() {
            return "Series{" +
                    "dbName='" + dbName + '\'' +
                    ", testName='" + testName + '\'' +
                    ", series=" + series +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException {
        copyChartToDataOuptutDir("test");
    }
}
