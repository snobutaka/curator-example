package com.github.snobutaka.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class GettingStarted {

    public CuratorFramework getConnection(String connectionString) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(/*baseSleepTimeMs=*/ 1000, /*maxRetries=*/3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();
        return client;
    }

    public String create(CuratorFramework client, String path, byte[] data) throws Exception {
        String createdPath = client.create().forPath(path, data);
        return createdPath;
    }

    /**
     * Use InterProcessMutex.
     * https://curator.apache.org/apidocs/org/apache/curator/framework/recipes/locks/InterProcessMutex.html
     *
     * @param client CuratorFramework instance
     * @param proc procedure to execute in critical section
     * @param lockPath znode use to lock
     * @return result of procedure
     * @throws Exception
     */
    public <T> T executeWithLock(CuratorFramework client, LockedProcedure<T> proc, String lockPath)
            throws Exception {

        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        try {
            lock.acquire();
            return proc.execute();
        } finally {
            lock.release();
        }
    }

    public static interface LockedProcedure<T> {
        public T execute();
    }
}
