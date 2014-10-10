package com.benstopford.nosql.databases.coherence;

import com.benstopford.nosql.DB;
import com.benstopford.nosql.util.Logger;
import com.benstopford.nosql.util.validators.RowValidator;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class Coherence implements DB {
    Logger log = Logger.instance();
    public static final Integer DEFAULT_PORT = 34739;
    public static final String DEFAULT_ADDRESS = "localhost";

    private NamedCache cache;

    @Override
    public void initialise() throws Exception {
        System.setProperty("client.extend.port", DEFAULT_PORT.toString());
        System.setProperty("client.extend.address", DEFAULT_ADDRESS.toString());
        cache = CacheFactory.getCacheFactoryBuilder()
                .getConfigurableCacheFactory("config/client.xml", ClassLoader.getSystemClassLoader())
                .ensureCache("test", ClassLoader.getSystemClassLoader());
        log.info("Connected...");
    }

    @Override
    public void clearDown() throws Exception {
        cache.clear();
    }

    @Override
    public void finalise() throws Exception {
        cache.destroy();
    }

    @Override
    public void load(Map<String, String> batch) throws Exception {
        cache.putAll(batch);
    }

    @Override
    public void read(Collection<String> keys, RowValidator rowValidator) throws Exception {
        Map all = cache.getAll(keys);
        for (Map.Entry entry : (Set<Map.Entry>) all.entrySet()) {
            rowValidator.validate(entry.getKey(), entry.getValue());
        }
    }
}
