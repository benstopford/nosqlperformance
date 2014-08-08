package com.benstopford.nosql.cassandra;

import com.benstopford.nosql.RunResult;
import com.benstopford.nosql.Runner;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

import java.nio.ByteBuffer;

/**
 * Hello world!
 */
public class CassandraRunner implements Runner {


    private Cluster cluster;
    private Session session;

    @Override
    public void initialise() {

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
        printConectedHosts(cluster.getMetadata());
        setupAfresh(session);
    }

    @Override
    public RunResult loadKeyValuePairs(long numberOfEntries, int entrySizeBytes, int entriesPerBatch) {
        byte[] data = new byte[entrySizeBytes];

        String blob = byteArrayToHex(data);

        System.out.println("Starting write");

        long start = System.currentTimeMillis();
        long totalBytesWritten = 0;
        long incrementalTime = System.currentTimeMillis();
        long lastTotalBytes = 0;

        StringBuilder statement = new StringBuilder();
        statement.append("begin batch\n");
        for (int i = 0; i < numberOfEntries; i++) {
            String string = "INSERT INTO test.data (id, secondary, data) " +
                    "VALUES (" + i + ",'some other index'," +
                    blob +
                    ");";
            statement.append(string+"\n");

            if(i% 1000 ==0){
                statement = applyBatch(session, statement);
            }

            totalBytesWritten+= blob.length();
            if (i % 100000 == 0) {
                long throughputToDate = totalBytesWritten * 1000 / (System.currentTimeMillis() - start);
                long througputInc = (totalBytesWritten-lastTotalBytes) * 1000 / (System.currentTimeMillis() - incrementalTime);
                lastTotalBytes = totalBytesWritten;
                incrementalTime = System.currentTimeMillis();
                System.out.printf("Written %,dB [%,d(hex)B/s][%,d(hex)B/s] %s\n", totalBytesWritten, throughputToDate, througputInc,i);
            }
        }
        applyBatch(session, statement);

        long took = System.currentTimeMillis() - start;
        System.out.println("Write All Took " + took + "ms");
        System.out.printf("wrote ids %s->%s\n", 0, numberOfEntries - 1);
        System.out.printf("Summary of write %,dB [%,d(hex)B/s]\n", totalBytesWritten, totalBytesWritten * 1000 / took);


        System.out.println();

        RunResult runResult = new RunResult("Load Cassandra");
        runResult.populate(numberOfEntries*entrySizeBytes, took, entrySizeBytes, numberOfEntries, entriesPerBatch);
        return runResult;
    }

    @Override
    public RunResult readKeyValuePairs(long numberOfEntries,int entriesPerBatch) {

        System.out.println("Starting read...");
        long start = System.currentTimeMillis();
        long totalRead = 0;
        long totalCount = 0;
        for (int i = 0; i < numberOfEntries; ) {
            StringBuffer s = new StringBuffer();
                   s.append( "SELECT id, data FROM test.data " +
                    "WHERE id in (");

            //add 100 ids to the where clause
            for (int j = 0; j < 100; j++) {
                if (i == numberOfEntries) break;
                s.append(" ");
                s.append(i++);
                s.append(", ");
            }
            s.append( " ");
            s.append(i++) ;
            s.append(");");

            ResultSet results = session.execute(s.toString());
            for (Row row : results) {
                byte[] entry = getData(row);
                totalCount++;
                totalRead += entry.length;
            }

            if (i % 10000 == 0) {
                System.out.println("Retrieved " + i + " ("+(totalRead *1000/(System.currentTimeMillis() - start)+"B/s)"));
            }

        }
        long took = System.currentTimeMillis() - start;
        System.out.println("Read All took " + took + "ms");
        System.out.println("returnedRowCount " + totalCount);


        RunResult runResult = new RunResult("Read Cassandra");
        runResult.populate(totalRead, took, -1, numberOfEntries, entriesPerBatch);
        return runResult;
    }

    @Override
    public void finalise() {
        cluster.close();
    }


    private StringBuilder applyBatch(Session session, StringBuilder statement) {
        statement.append("apply batch\n");
        session.execute(statement.toString());
        statement = new StringBuilder();
        statement.append("begin batch\n");
        return statement;
    }

    private void setupAfresh(Session session) {
        createKeyspace(session);
        dropExisting(session);
        session.execute(
                "CREATE TABLE test.data (" +
                        "id int PRIMARY KEY," +
                        "secondary text," +
                        "data blob" +
                        ");"
        );
        session.execute("truncate test.data;");
        ResultSet execute = session.execute("select count(*) from test.data");
        System.out.println("Table rowcount now: " + execute.iterator().next().getLong("count"));
    }


    private byte[] getData(Row row) {
        ByteBuffer data = row.getBytes("data");
        byte[] entry = new byte[data.remaining()];
        data.get(entry);
        return entry;
    }


    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2 + 2);
        sb.append("0x");
        for (byte b : a)
            sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }

    private void dropExisting(Session session) {
        try {

            session.execute("drop table test.data");
        } catch (Exception existsException) {

        }
    }

    private void createKeyspace(Session session) {
        try {

            session.execute("CREATE KEYSPACE test WITH replication " +
                    "= {'class':'SimpleStrategy', 'replication_factor':3};");

        } catch (AlreadyExistsException existsException) {

        }
    }

    private void printConectedHosts(Metadata metadata) {
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }

}
