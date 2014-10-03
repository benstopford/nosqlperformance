package com.benstopford.nosql.databases.couchbase;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.util.validators.RowValidator;
import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Logs: ~/Library/Application\ Support/Couchbase/var/lib/couchbase/logs
 * Exec: /Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core
 * Data: ~/Library/Application Support/Couchbase/var/lib/couchdb/default
 * Config: /Users/benji/Library/Application Support/Couchbase/var/lib/couchbase/config
 *
 * Using default couchbase install with one node and flush enabled.
 */

public class Couchbase implements DB {
    Logger log = Logger.getAnonymousLogger();

    private CouchbaseClient client;

    @Override
    public void initialise() throws Exception {
        List<URI> hosts = Arrays.asList(
                new URI("http://127.0.0.1:8091/pools")
        );
        client = new CouchbaseClient(hosts, "default", "");
    }

    @Override
    public void clearDown() throws Exception {
        log.info("clearing data");
        client.flush();
    }

    @Override
    public void finalise() throws Exception {
        client.shutdown(60, TimeUnit.SECONDS);
    }

    @Override
    public void load(Map<String, String> batch) throws Exception {
        for (String key : batch.keySet()) {
            String value = batch.get(key);
            OperationFuture<Boolean> result = client.set(key, value);
            if (!result.get()) {
                throw new RuntimeException("Failure occurred during write: " + result.getStatus().getMessage());
            }
            if (value == null) System.out.println("value was null");
        }
    }

    @Override
    public void read(Collection<String> keys, RowValidator rowValidator) throws Exception {
        for (String key : keys) {
            String row = (String) client.get(key);
            rowValidator.validate(key, row);
        }
    }
}
