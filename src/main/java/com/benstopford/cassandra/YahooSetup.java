package com.benstopford.cassandra;

import com.datastax.driver.core.Session;

public class YahooSetup {
    private static void setupForYahooCloudBench(Session session) {
        try {
            session.execute("CREATE KEYSPACE usertable WITH replication " +
                    "= {'class':'SimpleStrategy', 'replication_factor':3};");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        session.execute(
                "drop TABLE usertable.data;"
        );
        session.execute(
                "CREATE TABLE usertable.data (" +
                        "id BLOB PRIMARY KEY," +
                        ");"
        );
        System.out.println("done");
    }

}
