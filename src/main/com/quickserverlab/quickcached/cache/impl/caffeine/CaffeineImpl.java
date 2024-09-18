package com.quickserverlab.quickcached.cache.impl.caffeine;

import com.github.benmanes.caffeine.cache.*;
import com.quickserverlab.quickcached.QuickCached;
import com.quickserverlab.quickcached.cache.impl.BaseCacheImpl;

import java.io.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

class CacheValue implements Serializable {
    private final Object value;
    private final long duration;
    private final Instant creationTime;
    private static final long serialVersionUID = 1L;

    public CacheValue(Object value, long duration) {
        this.value = value;
        this.duration = duration;
        this.creationTime = Instant.now();
    }

    public Object getValue() {
        return value;
    }

    public long getDuration() {
        return duration;
    }

    public Instant getCreationTime() {
        return creationTime;
    }
}

public class CaffeineImpl extends BaseCacheImpl {
    private static final Logger logger = Logger.getLogger(CaffeineImpl.class.getName());

    private String dataPath = "./" + getName() + "_" + QuickCached.getPort() + ".dat";

    private Cache<String, CacheValue> cache = null;

    public CaffeineImpl() {
        FileInputStream inputStream = null;
        Properties config = null;
        String fileCfg = "./conf/caffeine/default.properties";
        try {
            config = new Properties();
            inputStream = new FileInputStream(fileCfg);
            config.load(inputStream);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not load[" + fileCfg + "] " + e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Failed to close the inputsteam: {0}", e);
                }
            }
        }

        try {
            Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder();

            long maxSize = 10_000;
            if (config != null) {
                String maxSizeStr = config.getProperty("maxsize");
                if (maxSizeStr != null) {
                    maxSize = Long.parseLong(maxSizeStr.trim());
                }

                String datapath = config.getProperty("datapath");
                if (datapath != null) {
                    dataPath = datapath;
                }
            }

            caffeineBuilder.maximumSize(maxSize);

            // use the dedicated, system-wided scheduling thread
            caffeineBuilder.scheduler(Scheduler.systemScheduler());

            // https://github.com/ben-manes/caffeine/wiki/Eviction#time-based
            cache = caffeineBuilder
                    .expireAfter(new Expiry<String, CacheValue>() {
                        @Override
                        // (key, value, currentTime)
                        public long expireAfterCreate(String s, CacheValue cacheValue, long l) {
                            long millis = cacheValue.getCreationTime()
                                    .plus(5, ChronoUnit.HOURS)
                                    .minus(System.currentTimeMillis(), ChronoUnit.MILLIS)
                                    .toEpochMilli();
                            return TimeUnit.MILLISECONDS.toNanos(millis);
                        }

                        @Override
                        // (key, value, currentTime, currentDuration)
                        public long expireAfterUpdate(String s, CacheValue cacheValue, long l, @org.checkerframework.checker.index.qual.NonNegative long l1) {
                            return l1;
                        }

                        @Override
                        public long expireAfterRead(String s, CacheValue cacheValue, long l, @org.checkerframework.checker.index.qual.NonNegative long l1) {
                            return l1;
                        }
                    }).build();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Initialization error: {0}", e);
        }
    }

    @Override
    public String getName() {
        return "CaffeineImpl";
    }

    @Override
    public boolean saveToDisk() {
        logger.log(Level.INFO, "Saving state to disk...");
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(dataPath));
            Map<String, CacheValue> cacheValueMap = cache.asMap();
            oos.writeObject(cacheValueMap);
            oos.flush();
            logger.log(Level.INFO, "Data has been successfully written to disk: {0}", dataPath);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to write data to disk: {0}", e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Failed to close the outputstream: {0}", e);
                }
            }
            logger.log(Level.INFO, "Done");
        }
        return true;
    }

    @Override
    public boolean readFromDisk() {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dataPath))) {
            Map<String, CacheValue> cacheValueMap = (Map<String, CacheValue>) ois.readObject();
            cache.putAll(cacheValueMap);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to read data from disk: {0}", e);
        }
        return true;
    }

    @Override
    public long getSize() {
        return cache.estimatedSize();
    }

    @Override
    public void setToCache(String key, Object value, int objectSize, int expInSec) throws Exception {
        long duration = TimeUnit.SECONDS.toNanos(expInSec);
        CacheValue cacheValue = new CacheValue(value, duration);
        cache.put(key, cacheValue);
    }

    @Override
    public void updateToCache(String key, Object value, int objectSize, int expInSec) throws Exception {
        setToCache(key, value, objectSize, expInSec);
    }

    @Override
    public void updateToCache(String key, Object value, int objectSize) throws Exception {
        //no action required here for ref based cache
    }

    @Override
    public Object getFromCache(String key) throws Exception {
        CacheValue cacheValue = cache.getIfPresent(key);
        if (cacheValue != null) {
            return cacheValue.getValue();
        } else {
            return null;
        }
    }

    @Override
    public boolean deleteFromCache(String key) throws Exception {
        cache.invalidate(key);
        return getFromCache(key) == null;
    }

    @Override
    public void flushCache() throws Exception {
        cache.invalidateAll();
    }
}
