package com.github.snobutaka.curator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.curator.framework.CuratorFramework;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import com.github.snobutaka.curator.GettingStarted.LockedProcedure;

public class GettingStartedTest {

    private static final int zkPort = 2181;
    private static String zkAddr;

    @ClassRule
    public static GenericContainer ZK =
        new GenericContainer<>("zookeeper:3.4.11")
            .withExposedPorts(zkPort);

    @BeforeClass
    public static void setUpBeforeClass() {
        zkAddr = "localhost:" + ZK.getMappedPort(zkPort);
    }
    /**
     * Test ZooKeeper 3.4.x compatibility.
     * http://curator.apache.org/zk-compatibility.html
     */
    @Test
    public void testCompatibilityMode() {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(zkAddr);
        assertThat(curator.isZk34CompatibilityMode(), is(true));
    }

    @Test
    public void testGetConnection() {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(zkAddr);
        assertThat(curator.isStarted(), is(true));
    }

    @Test
    public void testCreate() throws Exception {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(zkAddr);
        String path = "/getting_started_with_curator";
        byte[] data = "Hello Curator!".getBytes(StandardCharsets.UTF_8);
        String ret = gettingStarted.create(curator, path, data);
        assertThat(ret, is(path));
    }

    @Test
    public void testExecuteWithLock() throws InterruptedException, ExecutionException {
        final long waitMillis = 2 * 1000;
        final String lockPath = "/getting_started_with_curator_lock";
        ExecutorService executor = Executors.newFixedThreadPool(2);

        final GettingStarted gettingStarted1 = new GettingStarted();
        final CuratorFramework curator1 = gettingStarted1.getConnection(zkAddr);
        final GettingStarted gettingStarted2 = new GettingStarted();
        final CuratorFramework curator2 = gettingStarted2.getConnection(zkAddr);

        final List<String> target = Collections.synchronizedList(new ArrayList<String>());

        Future<String> futureOfProc1 = executor.submit(new Callable<String>() {
            public String call() throws Exception {
                LockedProcedure<String> proc = new LockedProcedure<String>() {
                    public String execute() {
                        try {
                            String data = "1";
                            Thread.sleep(waitMillis);
                            target.add(data);
                            return data;
                        } catch (InterruptedException e) {
                            return null;
                        }
                    }
                };
                return gettingStarted1.executeWithLock(curator1, proc, lockPath);
            }
        });

        Future<String> futureOfProc2 = executor.submit(new Callable<String>() {
            public String call() throws Exception {
                LockedProcedure<String> proc = new LockedProcedure<String>() {
                    public String execute() {
                        String data = "2";
                        target.add(data);
                        return data;
                    }
                };
                Thread.sleep(waitMillis / 2); // expect: curator1 acquired lock but not written data.
                return gettingStarted2.executeWithLock(curator2, proc, lockPath);
            }
        });

        final String val1 = futureOfProc1.get();
        assertThat(target.size(), is(1));
        assertThat(target.get(0), is(val1));

        final String val2 = futureOfProc2.get();
        assertThat(target.size(), is(2));
        assertThat(target.get(0), is(val1));
        assertThat(target.get(1), is(val2));
    }
}
