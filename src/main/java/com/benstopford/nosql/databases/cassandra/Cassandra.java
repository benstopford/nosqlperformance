package com.benstopford.nosql.databases.cassandra;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.util.validators.RowValidator;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;

/**
 Cassandra:
 Timeouts forced me to change in cassandra.yaml:
 read_request_timeout_in_ms: 30000
 range_request_timeout_in_ms: 50000

 /Users/benji/BensWorld/Dev/product-explorations/dsc-cassandra-2.0.6/bin

 */
public class Cassandra implements DB {
    public static final String ADDRESS = "127.0.0.1";

    private Cluster cluster;
    private Session session;

    @Override
    public void initialise() {

        cluster = Cluster.builder().addContactPoint(ADDRESS).build();
        session = cluster.connect();
        printConectedHosts(cluster.getMetadata());
        setupAfresh(session);
    }

    @Override
    public void clearDown() throws Exception {
        setupAfresh(session);
        session.close();
    }

    @Override
    public void load(Map<String, String> batch) {
        Collection keys = batch.keySet();

        StringBuilder statement = new StringBuilder();
        statement.append("BEGIN BATCH\n");

        for (Object key : keys) {
            //TODO remove some other index
               statement.append("INSERT INTO test.data (id, secondary) " +
                        "VALUES (" + key + ",'" +
                        batch.get(key) +
                        "');");
        }
        statement.append("\nAPPLY BATCH\n");
        session.execute(statement.toString());
    }

    @Override
    public void read(Collection keys, RowValidator rowValidator) {

        StringBuffer s = new StringBuffer();
        s.append("SELECT id, secondary FROM test.data " +
                "WHERE id in (");
        for (Object key : keys) {
            s.append(key);
            s.append(",");
        }
        s.setCharAt(s.length()-1,' ');
        s.append(");");


        ResultSet results = session.execute(s.toString());
        for (Row row : results) {
            Map.Entry entry = getEntry(row);
            rowValidator.validate(entry.getKey(), entry.getValue());
        }
    }

    private Map.Entry getEntry(Row row) {

        String key = String.valueOf(row.getInt("id"));
        String value = row.getString("secondary");

       return new  AbstractMap.SimpleEntry<String, String>(key,value);
    }

    @Override
    public void finalise() {
        cluster.close();
    }


    private StringBuilder applyBatch(Session session, StringBuilder statement) {

        statement.append("apply batch\n");
        session.execute(statement.toString());
        System.out.println(statement);
        statement = new StringBuilder();
        statement.append("begin batch\n");
        return statement;
    }

    private void setupAfresh(Session session) {
        assert (session!=null);
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
        System.out.println("Truncated table. Rowcount now: " + execute.iterator().next().getLong("count"));
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
