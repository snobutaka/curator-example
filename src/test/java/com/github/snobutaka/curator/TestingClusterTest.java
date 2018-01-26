package com.github.snobutaka.curator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class TestingClusterTest {

    @Test
    public void testTestingCluster() throws Exception {
        int instanceQty = 9;
        try (TestingCluster zkCluster = new TestingCluster(instanceQty)) {

            Collection<InstanceSpec> instances = zkCluster.getInstances();
            assertThat(instances.size(), is(instanceQty));

            zkCluster.start();
            CuratorFramework curator = new GettingStarted().getConnection(zkCluster.getConnectString());
            curator.blockUntilConnected();

            List<String> nodes = curator.getChildren().forPath("/");
            assertThat(nodes, is(Arrays.asList("zookeeper")));

            for (int i = 0; i < majority(instanceQty) - 1; i++) {
                ZooKeeper zk = curator.getZookeeperClient().getZooKeeper();
                InstanceSpec instance = zkCluster.findConnectionInstance(zk);
                zkCluster.killServer(instance);
                Thread.sleep(500);
                curator.blockUntilConnected();
                InstanceSpec newInstance = zkCluster.findConnectionInstance(zk);
                assertThat(instance, is(not(newInstance)));
            }
        }
    }

    @Test
    public void testMajority() {
        assertThat(majority(1), is(1));
        assertThat(majority(2), is(2));
        assertThat(majority(3), is(2));
        assertThat(majority(4), is(3));
        assertThat(majority(5), is(3));
        assertThat(majority(6), is(4));
        assertThat(majority(7), is(4));
    }

    int majority(int n) {
        return n / 2 + 1;
    }
}
