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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static org.apache.phoenix.query.logging.LoggingLFUCache.BEGIN_VALUES;
import static org.apache.phoenix.query.logging.LoggingLFUCache.END_VALUES;
import static org.apache.phoenix.query.logging.LoggingLFUCache.KEY_TOTAL_COUNT_SEPARATOR;
import static org.apache.phoenix.query.logging.StackTraceMappingInfo.BEGIN_CONN_INFO_VALS;
import static org.apache.phoenix.query.logging.StackTraceMappingInfo.CONN_INFO_VALS_SEPARATOR;
import static org.apache.phoenix.query.logging.StackTraceMappingInfo.END_CONN_INFO_VALS;
import static org.apache.phoenix.query.logging.StackTraceMappingInfo.STACK_LINES_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StackTraceMappingInfoTest {

    private static final int DEFAULT_LFU_CACHE_MAX_CAP = 5;
    private static final int DEFAULT_MAX_OPEN_PHX_CONNS = 10;
    private static final long LONG_CONN_OPEN_THRESHOLD = 1000000L;
    private static final long SHORT_CONN_OPEN_THRESHOLD = 1L;
    private static final long DUMMY_TIME1 = 1000L;
    private static final long DUMMY_TIME2 = 100L;
    private static final String ESC_BEG_ST_VALS = Pattern.quote(BEGIN_VALUES);
    private static final String ESC_END_ST_VALS = Pattern.quote(END_VALUES);
    private static final String ESC_ST_TOTAL_COUNT_SEP = Pattern.quote(KEY_TOTAL_COUNT_SEPARATOR);
    private static final String ESC_BEG_CI_VALS = Pattern.quote(BEGIN_CONN_INFO_VALS);
    private static final String ESC_CI_VALS_SEPS = Pattern.quote(CONN_INFO_VALS_SEPARATOR);
    private static final String ESC_END_CI_VALS = Pattern.quote(END_CONN_INFO_VALS);
    private static final String ESC_ST_LINES_SEP = Pattern.quote(STACK_LINES_SEPARATOR);

    @Test
    public void testBasicConnectionInfoStackTraceMaps() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        final UUID dummyId1 = UUID.randomUUID();
        final UUID dummyId2 = UUID.randomUUID();
        stMappingObj.registerConnectionOpened(dummyId1, DUMMY_TIME1);
        stMappingObj.registerConnectionOpened(dummyId2, DUMMY_TIME2);

        StackTraceMappingInfo.ConnInfo conn1 = getConnInfoFromStMapInfo(stMappingObj, dummyId1);
        StackTraceMappingInfo.ConnInfo conn2 = getConnInfoFromStMapInfo(stMappingObj, dummyId2);

        assertEquals(DUMMY_TIME1, conn1.getConnOpenedTime());
        assertEquals(DUMMY_TIME2, conn2.getConnOpenedTime());
        assertEquals(conn1.getThreadID(), conn2.getThreadID());
        LoggingCache<Integer, String> cachedStackTraces = stMappingObj.getStackTraceMap();

        assertNotNull(cachedStackTraces.get(conn1.getHashStackTrace()));
        assertNotNull(cachedStackTraces.get(conn2.getHashStackTrace()));

        // Close both connections and check map contents again
        stMappingObj.registerConnectionClosed(dummyId1, DUMMY_TIME1 + 1);
        stMappingObj.registerConnectionClosed(dummyId2, DUMMY_TIME2 + 1);
        conn1 = getConnInfoFromStMapInfo(stMappingObj, dummyId1);
        conn2 = getConnInfoFromStMapInfo(stMappingObj, dummyId2);

        assertNull(conn1);
        assertNull(conn2);
    }

    @Test
    public void testSameStackTraceRecordedOnlyOnce() {
        int NUM_CONNS = 10;
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                NUM_CONNS, LONG_CONN_OPEN_THRESHOLD);
        List<UUID> uniquePhxConns = new ArrayList<>(NUM_CONNS);
        for (int i = 0; i < NUM_CONNS; i++) {
            UUID newId = UUID.randomUUID();
            uniquePhxConns.add(newId);
            // Register open from the same location so each connection gets the same stack trace
            stMappingObj.registerConnectionOpened(newId, DUMMY_TIME1);
        }
        LoggingCache<Integer, String> cachedStackTraces = stMappingObj.getStackTraceMap();
        assertEquals("Only 1 stack trace should be stored", 1, cachedStackTraces.size());
        StackTraceMappingInfo.ConnInfo connInfo =
                getConnInfoFromStMapInfo(stMappingObj, uniquePhxConns.get(0));
        int hashedSt = connInfo.getHashStackTrace();

        assertNotNull(cachedStackTraces.get(hashedSt));
        for (int i = 1; i < uniquePhxConns.size(); i++) {
            // Each Phoenix connection should map to the same stack trace
            assertEquals(hashedSt, getConnInfoFromStMapInfo(stMappingObj,
                    uniquePhxConns.get(i)).getHashStackTrace());
        }
    }

    @Test
    public void testDiffStackTracesRecordedSeparately() {
        createTwoConnectionsAndAssertMapContents();
    }

    @Test
    public void testNullUUIDFails() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        try {
            stMappingObj.registerConnectionOpened(null, DUMMY_TIME1);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }
        try {
            stMappingObj.registerConnectionClosed(null, DUMMY_TIME2);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }
    }

    @Test
    public void testRegisterCloseBeforeOpenNoOp() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        stMappingObj.registerConnectionClosed(UUID.randomUUID(), DUMMY_TIME1);
        assertTrue(stMappingObj.getOpenPhoenixConnectionsMap().isEmpty());
        assertEquals(0, stMappingObj.getStackTraceMap().size());
    }

    @Test
    public void testClearState() {
        StackTraceMappingInfo stMappingObj = createTwoConnectionsAndAssertMapContents();
        stMappingObj.clearAllState();
        assertTrue(stMappingObj.getOpenPhoenixConnectionsMap().isEmpty());
        assertEquals(0, stMappingObj.getStackTraceMap().size());
    }

    @Test
    public void testStackTraceMapString() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        for (int i = 0; i < 2; i++) {
            UUID newId = UUID.randomUUID();
            // These 2 connections will have the same stack trace, open time and caller thread
            stMappingObj.registerConnectionOpened(newId, DUMMY_TIME1);
        }
        UUID newId2 = UUID.randomUUID();
        stMappingObj.registerConnectionOpened(newId2, DUMMY_TIME2);
        // Closing the connection does not affect the hash(ST)->Stack trace map
        stMappingObj.registerConnectionClosed(newId2, DUMMY_TIME2 + 3);

        // Test ConnInfo strings
        StringBuilder sb1 = new StringBuilder();
        String connInfoMapString = stMappingObj.getOpenConnInfoMapString(sb1);

        String[] ciStrArray = connInfoMapString.split(ESC_END_CI_VALS);
        List<String> connInfoStrings = new ArrayList<>();
        for (String s : ciStrArray) {
            connInfoStrings.add(s.split(ESC_BEG_CI_VALS)[1]);
        }
        ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhoenixConnsMap =
                stMappingObj.getOpenPhoenixConnectionsMap();
        // We close the third connection, so it shouldn't be there
        assertEquals(2, connInfoStrings.size());
        assertEquals(openPhoenixConnsMap.size(), connInfoStrings.size());

        String[] ci1StringParts = connInfoStrings.get(0).split(ESC_CI_VALS_SEPS);
        String[] ci2StringParts = connInfoStrings.get(1).split(ESC_CI_VALS_SEPS);
        assertEquals(3, ci1StringParts.length);
        assertEquals(3, ci2StringParts.length);
        assertEquals(ci1StringParts[0], ci2StringParts[0]);
        for (StackTraceMappingInfo.ConnInfo connInfo : openPhoenixConnsMap.values()) {
            assertEquals(String.valueOf(connInfo.getHashStackTrace()), ci1StringParts[1]);
            assertEquals(String.valueOf(connInfo.getHashStackTrace()), ci2StringParts[1]);
            assertEquals(String.valueOf(connInfo.getThreadID()), ci1StringParts[2]);
            assertEquals(String.valueOf(connInfo.getThreadID()), ci2StringParts[2]);
        }

        // Test cached stack trace Strings
        StringBuilder sb2 = new StringBuilder();
        String mostFreqStString = stMappingObj.getMostFreqStackTraces(sb2);
        LoggingCache<Integer, String> cachedStackTraces = stMappingObj.getStackTraceMap();

        String[] stackTraceStrings = mostFreqStString.split(ESC_END_ST_VALS);
        // Should contain 2 stack traces
        assertEquals(2, stackTraceStrings.length);
        assertEquals(cachedStackTraces.size(), stackTraceStrings.length);

        String[] st1StringParts = stackTraceStrings[0].split(ESC_ST_TOTAL_COUNT_SEP)[0].split(ESC_BEG_ST_VALS);
        String[] st2StringParts = stackTraceStrings[1].split(ESC_ST_TOTAL_COUNT_SEP)[0].split(ESC_BEG_ST_VALS);
        assertEquals(2, st1StringParts.length);
        assertEquals(2, st2StringParts.length);
    }

    @Test
    public void testGetCallerStackTrace() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        StackTraceElement[] st = Thread.currentThread().getStackTrace();
        String[] sTStringElems = stMappingObj.getCallerStackTrace(0, st).split(ESC_ST_LINES_SEP);

        assertEquals(st.length, sTStringElems.length);
        for (int i = 0; i < sTStringElems.length; i++) {
            assertEquals(st[i].toString(), sTStringElems[i]);
        }
    }

    @Test
    public void testStackTraceMapStaysWithinMaxCapacity() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(2,
                DEFAULT_MAX_OPEN_PHX_CONNS, LONG_CONN_OPEN_THRESHOLD);
        for (int i = 0; i < 5; i++) {
            stMappingObj.registerConnectionOpened(UUID.randomUUID(), DUMMY_TIME1);
        }
        for (int i = 0; i < 3; i++) {
            stMappingObj.registerConnectionOpened(UUID.randomUUID(), DUMMY_TIME2);
        }
        // Of the 3 different stack traces, this one will be evicted since max_capacity is 2
        for (int i = 0; i < 1; i++) {
            stMappingObj.registerConnectionOpened(UUID.randomUUID(), DUMMY_TIME1 + 100);
        }
        LoggingCache<Integer, String> stMap = stMappingObj.getStackTraceMap();
        assertTrue(stMap.isFull());
        assertEquals(2, stMap.size());
    }

    @Test
    public void testStopStoringConnectionsAfterMaxOpenConnsLimit() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                2, LONG_CONN_OPEN_THRESHOLD);
        List<UUID> uniquePhxConns = new ArrayList<>();
        // The third connection will be ignored
        for (int i = 0; i < 3; i++) {
            UUID newId = UUID.randomUUID();
            uniquePhxConns.add(newId);
            stMappingObj.registerConnectionOpened(newId, DUMMY_TIME1);
        }
        ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhxConnsMap =
                stMappingObj.getOpenPhoenixConnectionsMap();
        assertEquals(2, openPhxConnsMap.size());
        assertTrue(openPhxConnsMap.containsKey(uniquePhxConns.get(0)));
        assertTrue(openPhxConnsMap.containsKey(uniquePhxConns.get(1)));
        assertFalse(openPhxConnsMap.containsKey(uniquePhxConns.get(2)));

        // Now close one of the connections and try opening the 3rd one again
        stMappingObj.registerConnectionClosed(uniquePhxConns.get(0), DUMMY_TIME1 + 1);
        stMappingObj.registerConnectionOpened(uniquePhxConns.get(2), DUMMY_TIME1);

        assertEquals(2, openPhxConnsMap.size());
        assertFalse(openPhxConnsMap.containsKey(uniquePhxConns.get(0)));
        assertTrue(openPhxConnsMap.containsKey(uniquePhxConns.get(1)));
        assertTrue(openPhxConnsMap.containsKey(uniquePhxConns.get(2)));
    }

    @Test
    public void testLogLongOpenConnectionsOnClose() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP,
                2, SHORT_CONN_OPEN_THRESHOLD);
        StackTraceMappingInfo stMappingObjSpy = Mockito.spy(stMappingObj);
        UUID newId = UUID.randomUUID();
        stMappingObjSpy.registerConnectionOpened(newId, DUMMY_TIME1);
        stMappingObjSpy.registerConnectionClosed(newId, DUMMY_TIME1 + 2);

        Mockito.verify(stMappingObjSpy, Mockito.times(1)).
                logIfConnectionKeptOpenForTooLong(Mockito.any(StackTraceMappingInfo.ConnInfo.class),
                        Mockito.anyLong());
    }

    @Test
    public void testConnsInDifferentCallerThreads() throws InterruptedException {
        final StackTraceMappingInfo stMappingObj =
                getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP, DEFAULT_MAX_OPEN_PHX_CONNS,
                        LONG_CONN_OPEN_THRESHOLD);
        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                stMappingObj.registerConnectionOpened(id1, DUMMY_TIME1);
            }
        });
        t1.start();
        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                stMappingObj.registerConnectionOpened(id2, DUMMY_TIME2);
            }
        });
        t2.start();

        t1.join();
        t2.join();
        StackTraceMappingInfo.ConnInfo connInfo1 = getConnInfoFromStMapInfo(stMappingObj, id1);
        StackTraceMappingInfo.ConnInfo connInfo2 = getConnInfoFromStMapInfo(stMappingObj, id2);
        assertEquals(t1.getId(), connInfo1.getThreadID());
        assertEquals(DUMMY_TIME1, connInfo1.getConnOpenedTime());

        assertEquals(t2.getId(), connInfo2.getThreadID());
        assertEquals(DUMMY_TIME2, connInfo2.getConnOpenedTime());
    }

    @Test
    public void testConcurrentConnOpenAndClose() throws InterruptedException {
        final StackTraceMappingInfo stMappingObj =
                getStackTraceMappingInfoObj(DEFAULT_LFU_CACHE_MAX_CAP, DEFAULT_MAX_OPEN_PHX_CONNS,
                        LONG_CONN_OPEN_THRESHOLD);
        final List<UUID> threadOneIds = getUUIDListOfSize(3);
        final List<UUID> threadTwoIds = getUUIDListOfSize(5);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                for (UUID id : threadOneIds) {
                    stMappingObj.registerConnectionOpened(id, DUMMY_TIME1);
                }
            }
        });
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                for (UUID id : threadTwoIds) {
                    stMappingObj.registerConnectionOpened(id, DUMMY_TIME2);
                }
                stMappingObj.registerConnectionClosed(threadTwoIds.get(threadTwoIds.size() - 1),
                        DUMMY_TIME2 + 1);
            }
        });
        t2.start();

        t1.join();
        t2.join();
        // To test hash(stack trace) behavior when created from different threads
        assertEquals(2, stMappingObj.getStackTraceMap().size());
        int firstConnHash =
                getConnInfoFromStMapInfo(stMappingObj, threadOneIds.get(0)).getHashStackTrace();
        for (UUID threadOneId : threadOneIds) {
            StackTraceMappingInfo.ConnInfo connInfo =
                    getConnInfoFromStMapInfo(stMappingObj, threadOneId);
            assertEquals(firstConnHash, connInfo.getHashStackTrace());
            assertEquals(t1.getId(), connInfo.getThreadID());
            assertEquals(DUMMY_TIME1, connInfo.getConnOpenedTime());
        }

        firstConnHash =
                getConnInfoFromStMapInfo(stMappingObj, threadTwoIds.get(0)).getHashStackTrace();
        // Last connection is closed
        for (int i = 0; i < threadTwoIds.size() - 1; i++) {
            StackTraceMappingInfo.ConnInfo connInfo = getConnInfoFromStMapInfo(stMappingObj,
                    threadTwoIds.get(i));
            assertEquals(firstConnHash, connInfo.getHashStackTrace());
            assertEquals(t2.getId(), connInfo.getThreadID());
            assertEquals(DUMMY_TIME2, connInfo.getConnOpenedTime());
        }
    }

    private List<UUID> getUUIDListOfSize(int size) {
        List<UUID> uuidList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            uuidList.add(UUID.randomUUID());
        }
        return uuidList;
    }

    private StackTraceMappingInfo getStackTraceMappingInfoObj(int lfuCacheMaxCap,
            int maxOpenPhoenixConns, long connOpenTooLongWarnThresholdMs) {
        return new StackTraceMappingInfo(lfuCacheMaxCap, maxOpenPhoenixConns,
                connOpenTooLongWarnThresholdMs);
    }

    private StackTraceMappingInfo.ConnInfo getConnInfoFromStMapInfo(
            StackTraceMappingInfo stMapInfo, UUID phxConnId) {
        return stMapInfo.getOpenPhoenixConnectionsMap().get(phxConnId);
    }

    private StackTraceMappingInfo createTwoConnectionsAndAssertMapContents() {
        StackTraceMappingInfo stMappingObj = getStackTraceMappingInfoObj(3,
                3, LONG_CONN_OPEN_THRESHOLD);
        UUID newId1 = UUID.randomUUID();
        UUID newId2 = UUID.randomUUID();
        // Get different stack traces for both
        stMappingObj.registerConnectionOpened(newId1, DUMMY_TIME1);
        stMappingObj.registerConnectionOpened(newId2, DUMMY_TIME2);
        LoggingCache<Integer, String> cachedStackTraces = stMappingObj.getStackTraceMap();
        assertEquals("Different stack traces should be stored", 2, cachedStackTraces.size());

        Integer hashSt1 = getConnInfoFromStMapInfo(stMappingObj, newId1).getHashStackTrace();
        Integer hashSt2 = getConnInfoFromStMapInfo(stMappingObj, newId2).getHashStackTrace();
        assertNotEquals(hashSt1, hashSt2);
        assertNotNull(cachedStackTraces.get(hashSt1));
        assertNotNull(cachedStackTraces.get(hashSt2));
        return stMappingObj;
    }

}