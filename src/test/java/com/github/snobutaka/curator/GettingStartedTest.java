package com.github.snobutaka.curator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.snobutaka.curator.GettingStarted.LockedProcedure;

public class GettingStartedTest {

    TestingServer zookeeperServer;

    @Before
    public void setUp() throws Exception {
        this.zookeeperServer = new TestingServer();
        this.zookeeperServer.start();
    }

    @After
    public void tearDown() throws IOException {
        if (this.zookeeperServer != null) {
            this.zookeeperServer.close();
        }
    }

    /**
     * Test ZooKeeper 3.4.x compatibility.
     * http://curator.apache.org/zk-compatibility.html
     */
    @Test
    public void testCompatibilityMode() {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(this.zookeeperServer.getConnectString());
        assertThat(curator.isZk34CompatibilityMode(), is(true));
    }

    @Test
    public void testGetConnection() throws InterruptedException {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(this.zookeeperServer.getConnectString());
        curator.blockUntilConnected();
        assertThat(curator.getState(), is(CuratorFrameworkState.STARTED));
    }

    @Test
    public void testCuratorRetursZooKeeperClient() throws Exception {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(this.zookeeperServer.getConnectString());
        CuratorZookeeperClient curatorZkClient = curator.getZookeeperClient();
        ZooKeeper zkClient = curatorZkClient.getZooKeeper();

        curator.blockUntilConnected();
        assertThat(curator.getState(), is(CuratorFrameworkState.STARTED));
        assertThat(curatorZkClient.isConnected(), is(true));
        assertThat(zkClient.getState(), is(States.CONNECTED));
    }

    @Test
    public void testCreate() throws Exception {
        GettingStarted gettingStarted = new GettingStarted();
        CuratorFramework curator = gettingStarted.getConnection(this.zookeeperServer.getConnectString());
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
        final CuratorFramework curator1 = gettingStarted1.getConnection(this.zookeeperServer.getConnectString());
        final GettingStarted gettingStarted2 = new GettingStarted();
        final CuratorFramework curator2 = gettingStarted2.getConnection(this.zookeeperServer.getConnectString());

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
