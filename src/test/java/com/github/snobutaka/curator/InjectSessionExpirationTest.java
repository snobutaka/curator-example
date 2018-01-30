package com.github.snobutaka.curator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.utils.InjectSessionExpiration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class InjectSessionExpirationTest extends TestBase {

    @Test
    public void test() throws IOException, KeeperException, InterruptedException {
        final List<String> data = new ArrayList<>();
        Watcher expirationWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.Expired) {
                    data.add("expired");
                }
            }
        };
        ZooKeeper zookeeperClient = new ZooKeeper(zookeeperServer.getConnectString(), 10000, expirationWatcher);
        zookeeperClient.getData("/", false, null);

        InjectSessionExpiration.injectSessionExpiration(zookeeperClient);
        Thread.sleep(500);
        assertThat(data, is(Arrays.asList("expired")));
    }
}
