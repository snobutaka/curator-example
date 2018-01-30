package com.github.snobutaka.curator;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

public class TestBase {

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
}
