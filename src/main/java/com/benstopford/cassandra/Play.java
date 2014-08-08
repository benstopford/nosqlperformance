package com.benstopford.cassandra;

import com.datastax.driver.core.*;

/**
 * Hello world!
 */
public class Play {
    public static void main(String[] args) {
        new Play();
    }

    public Play() {

        //Setup Cluster
        Cluster cluster;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();
        printConectedHosts(cluster.getMetadata());


        System.out.println("Starting read...");
        long start = System.currentTimeMillis();

            String s = "SELECT count(id) FROM test.data ";

            ResultSet results = session.execute(s);
            for (Row row : results) {
                System.out.println(row);
            }

        System.out.println("Took "+(System.currentTimeMillis()-start));

        cluster.close();
    }


    private void printConectedHosts(Metadata metadata) {
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }
}
