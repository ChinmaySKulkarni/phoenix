/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query.logging;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.phoenix.query.QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.CONN_OPEN_TOO_LONG_WARN_THRESHOLD_MS;
import static org.apache.phoenix.query.QueryServices.MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED;
import static org.apache.phoenix.query.QueryServices.MAX_CONCURRENT_CONN_LFU_CACHE_MAX_CAPACITY;
import static org.apache.phoenix.query.QueryServicesOptions
        .DEFAULT_MAX_CONCURRENT_CONN_EXTRA_LOGGING_SAMPLING_RATE;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MaxConcurrentConnectionsIT extends BaseUniqueNamesOwnClusterIT {

    private static final String RAND_STRING = "A";

    @BeforeClass
    public static void setUp() throws Exception {
        HBaseTestingUtility hbaseTestUtil = new HBaseTestingUtility();
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkQuorum +
                JDBC_PROTOCOL_SEPARATOR + "uniqueConn=A";
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @Before
    // Keep appending a random character to the url so that we always use a different CQS
    // object rather than the one cached in PhoenixDriver amongst different test runs
    public void modifyURL() {
        url += RAND_STRING;
    }

    @Test
    public void testMaxConcurrentConnLogging() throws SQLException {
        int attemptedPhoenixConnections = 11;
        int maxConnections = attemptedPhoenixConnections - 1;
        List<Connection> connections = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                Integer.toString(maxConnections));
        props.setProperty(MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED, Boolean.toString(true));
        // Log on every exception
        props.setProperty(QueryServices.MAX_CONCURRENT_CONN_EXTRA_LOGGING_SAMPLING_RATE, "1");
        props.setProperty(MAX_CONCURRENT_CONN_LFU_CACHE_MAX_CAPACITY,
                Integer.toString(maxConnections));

        MaxConcurrentConnectionInfo maxConcConnInfo = null;
        boolean wasThrottled = false;
        try {
            for (int k = 0; k < attemptedPhoenixConnections; k++) {
                Connection c = DriverManager.getConnection(url, props);
                connections.add(c);
                // Capture just before we throw the exception
                if (connections.size() == maxConnections) {
                    ConnectionQueryServicesImpl qs = (ConnectionQueryServicesImpl)c
                            .unwrap(PhoenixConnection.class).getQueryServices();
                    maxConcConnInfo = qs.getMaxConcurrentConnectionInfo();
                    assertValuesForOpenedConns(maxConcConnInfo, connections,
                            Thread.currentThread().getId());
                }
            }
            fail();
        } catch (SQLException se) {
            wasThrottled = true;
            assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(),
                    se.getErrorCode());
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
        assertTrue("No connection was throttled!", wasThrottled);
        assertEquals(maxConnections, connections.size());
        assertValuesForClosedConns(maxConcConnInfo, connections);
    }

    @Test
    public void testSamplingRate() throws SQLException {
        int attemptedPhoenixConnections = 12;
        int maxConnections = attemptedPhoenixConnections/6;
        // Should log 3 times since 6 exceptions will be thrown
        int samplingRate = 2;
        List<Connection> connections = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                Integer.toString(maxConnections));
        props.setProperty(MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED, Boolean.toString(true));
        props.setProperty(QueryServices.MAX_CONCURRENT_CONN_EXTRA_LOGGING_SAMPLING_RATE,
                Integer.toString(samplingRate));
        props.setProperty(MAX_CONCURRENT_CONN_LFU_CACHE_MAX_CAPACITY,
                Integer.toString(maxConnections));

        MaxConcurrentConnectionInfo maxConcConnInfo = null;
        int exceptionCount = 0;
        int numSamples = 0;
        try {
            for (int i = 0; i < attemptedPhoenixConnections; i++) {
                try {
                    Connection c = (DriverManager.getConnection(url, props));
                    connections.add(c);
                    if (maxConcConnInfo == null) {
                        ConnectionQueryServicesImpl qs = (ConnectionQueryServicesImpl)
                                c.unwrap(PhoenixConnection.class).getQueryServices();
                        maxConcConnInfo = qs.getMaxConcurrentConnectionInfo();
                    }
                } catch (SQLException se) {
                    exceptionCount++;
                    if (maxConcConnInfo != null) {
                        LoggingCache<Integer, String> stMap =
                                ((StackTraceMappingInfo)maxConcConnInfo).getStackTraceMap();
                        ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhxConnsMap =
                                ((StackTraceMappingInfo)maxConcConnInfo).
                                        getOpenPhoenixConnectionsMap();

                        // We should have logged everything and cleared all data structures
                        if (exceptionCount % samplingRate == 0) {
                            assertEquals(0, stMap.size());
                            assertTrue(openPhxConnsMap.isEmpty());
                            numSamples++;
                        } else {
                            assertNotEquals(0, stMap.size());
                            assertFalse(openPhxConnsMap.isEmpty());
                        }
                    } else {
                        fail("Not even 1 connection allowed to be created! Reduce throttling.");
                    }
                    assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(),
                            se.getErrorCode());
                    for (Connection c : connections) {
                        c.close();
                    }
                    connections.clear();
                }
            }
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
        assertEquals(exceptionCount/samplingRate, numSamples);
    }

    @Test
    public void testConfGetsSetCorrectly() throws SQLException {
        int maxAllowedConns = 10;
        int lfuCacheMaxCap = 5;
        long warnThreshMs = 100000L;

        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                Integer.toString(maxAllowedConns));
        props.setProperty(MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED, Boolean.toString(true));
        // Provide bogus value which should get set as the default
        props.setProperty(QueryServices.MAX_CONCURRENT_CONN_EXTRA_LOGGING_SAMPLING_RATE,
                Integer.toString(-1));
        props.setProperty(MAX_CONCURRENT_CONN_LFU_CACHE_MAX_CAPACITY,
                Integer.toString(lfuCacheMaxCap));
        props.setProperty(CONN_OPEN_TOO_LONG_WARN_THRESHOLD_MS,
                Long.toString(warnThreshMs));

        try (Connection c = DriverManager.getConnection(url, props)) {
            ConnectionQueryServicesImpl qs = (ConnectionQueryServicesImpl) (c)
                    .unwrap(PhoenixConnection.class).getQueryServices();
            StackTraceMappingInfo maxConcConnInfo =
                    (StackTraceMappingInfo)qs.getMaxConcurrentConnectionInfo();
            assertEquals(lfuCacheMaxCap, maxConcConnInfo.getLfuCacheMaxCapacity());
            assertEquals(maxAllowedConns, maxConcConnInfo.getMaxOpenPhoenixConnections());
            assertEquals(warnThreshMs, maxConcConnInfo.getConnectionOpenTooLongWarnThresholdMs());
            assertEquals(DEFAULT_MAX_CONCURRENT_CONN_EXTRA_LOGGING_SAMPLING_RATE,
                    qs.getMaxConcurrentConnExtraLogSamplingRate());
        }
    }

    @Test
    public void testLoggingOffWorks() throws SQLException {
        int attemptedPhoenixConnections = 11;
        int maxConnections = attemptedPhoenixConnections - 1;
        List<Connection> connections = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                Integer.toString(maxConnections));
        props.setProperty(MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED, Boolean.toString(false));

        try {
            for (int k = 0; k < attemptedPhoenixConnections; k++) {
                Connection c = (DriverManager.getConnection(url, props));
                connections.add(c);
                // Capture just before we throw the exception
                if (k == maxConnections) {
                    ConnectionQueryServicesImpl qs = (ConnectionQueryServicesImpl)
                            c.unwrap(PhoenixConnection.class).getQueryServices();
                    assertNull(qs.getMaxConcurrentConnectionInfo());
                }
            }
            fail();
        } catch (SQLException se) {
            assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(),
                    se.getErrorCode());
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
    }

    @Test
    public void testConcurrentConnOpenAndCloseLogging() throws InterruptedException, SQLException {
        final int attemptedConnsThread1 = 10;
        final int attemptedConnsThread2 = 7;
        final int maxAllowedConns = attemptedConnsThread1 + attemptedConnsThread2 - 1;
        final Properties p = new Properties();
        p.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                Integer.toString(maxAllowedConns));
        p.setProperty(MAX_CONCURRENT_CONN_EXTRA_LOGGING_ENABLED, Boolean.toString(true));
        ConnectionCreateCloseRunnable connCCR1 =
                new ConnectionCreateCloseRunnable(attemptedConnsThread1, p);
        ConnectionCreateCloseRunnable connCCR2 =
                new ConnectionCreateCloseRunnable(attemptedConnsThread2, p);

        Thread t1 = new Thread(connCCR1);
        t1.start();
        Thread t2 = new Thread(connCCR2);
        t2.start();

        t1.join();
        t2.join();
        assertEquals((attemptedConnsThread1 + attemptedConnsThread2) - maxAllowedConns,
                connCCR1.getNumExceptions() + connCCR2.getNumExceptions());
        List<Connection> firstThreadConnList = connCCR1.getConnectionList();
        List<Connection> secondThreadConnList = connCCR2.getConnectionList();

        assertValuesForOpenedConns(connCCR1.getMaxConcConnInfo(), firstThreadConnList, t1.getId());
        assertValuesForOpenedConns(connCCR2.getMaxConcConnInfo(), secondThreadConnList, t2.getId());

        for (Connection c : firstThreadConnList) {
            c.close();
        }
        for (Connection c : secondThreadConnList) {
            c.close();
        }
        assertValuesForClosedConns(connCCR1.getMaxConcConnInfo(), firstThreadConnList);
        assertValuesForClosedConns(connCCR2.getMaxConcConnInfo(), secondThreadConnList);
    }

    private static class ConnectionCreateCloseRunnable implements Runnable {

        private final int attemptedConns;
        private final List<Connection> connectionList = new ArrayList<>();
        private MaxConcurrentConnectionInfo maxConcConnInfo = null;
        private final Properties props;
        private int numExceptions = 0;

        public ConnectionCreateCloseRunnable(int attemptedConns, Properties props) {
            this.attemptedConns = attemptedConns;
            this.props = props;
        }

        @Override public void run() {
            for (int i = 0; i < this.attemptedConns; i++) {
                try {
                    Connection conn = DriverManager.getConnection(url, this.props);
                    if (this.maxConcConnInfo == null) {
                        this.maxConcConnInfo = ((ConnectionQueryServicesImpl)conn
                                .unwrap(PhoenixConnection.class)
                                .getQueryServices())
                                .getMaxConcurrentConnectionInfo();
                    }
                    this.connectionList.add(conn);
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(),
                            e.getErrorCode());
                    numExceptions++;
                }
            }
        }

        public List<Connection> getConnectionList() {
            return this.connectionList;
        }

        public int getNumExceptions() {
            return this.numExceptions;
        }

        public MaxConcurrentConnectionInfo getMaxConcConnInfo() {
            return this.maxConcConnInfo;
        }
    }

    private void assertValuesForOpenedConns(MaxConcurrentConnectionInfo maxConcConnInfo,
            List<Connection> openConns, long threadId) throws SQLException {
        if (openConns.isEmpty()) {
            return;
        }
        LoggingCache<Integer, String> stMap =
                ((StackTraceMappingInfo)maxConcConnInfo).getStackTraceMap();
        ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhxConnsMap =
                ((StackTraceMappingInfo)maxConcConnInfo).getOpenPhoenixConnectionsMap();

        PhoenixConnection firstConn = openConns.get(0).unwrap(PhoenixConnection.class);
        StackTraceMappingInfo.ConnInfo firstConnInfo = openPhxConnsMap.get(firstConn.getUniqueID());
        for (Connection c : openConns) {
            UUID id = c.unwrap(PhoenixConnection.class).getUniqueID();
            StackTraceMappingInfo.ConnInfo connInfo = openPhxConnsMap.get(id);
            assertNotNull(connInfo);
            assertEquals(firstConnInfo.getHashStackTrace(), connInfo.getHashStackTrace());
            assertEquals(firstConnInfo.getThreadID(), connInfo.getThreadID());
            assertEquals(threadId, connInfo.getThreadID());
            assertNotNull(stMap.get(connInfo.getHashStackTrace()));
        }
    }

    private void assertValuesForClosedConns(MaxConcurrentConnectionInfo maxConcConnInfo,
            List<Connection> closedConns) throws SQLException {
        ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhxConnsMap =
                ((StackTraceMappingInfo)maxConcConnInfo).getOpenPhoenixConnectionsMap();
        for (Connection c : closedConns) {
            UUID id = c.unwrap(PhoenixConnection.class).getUniqueID();
            assertNull(openPhxConnsMap.get(id));
        }
    }

}