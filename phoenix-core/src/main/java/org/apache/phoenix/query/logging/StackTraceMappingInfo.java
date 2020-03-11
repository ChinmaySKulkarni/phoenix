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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation used to store the caller stack trace information for opened Phoenix connections
 */
public class StackTraceMappingInfo implements MaxConcurrentConnectionInfo {

    private final int lfuCacheMaxCapacity;
    private final int maxOpenPhoenixConnections;
    private final long connectionOpenTooLongWarnThresholdMs;
    private final Object stackTraceMapLock = new Object();
    private final Object openPhoenixConnectionsMapLock = new Object();
    // LFU cache containing hash(stackTrace) to String stack trace mappings
    @GuardedBy("stackTraceMapLock")
    private final LoggingCache<Integer, String> stackTraceMap;
    // Map Phoenix connection UUID to its connection info object
    @GuardedBy("openPhoenixConnectionsMapLock")
    private final ConcurrentHashMap<UUID, StackTraceMappingInfo.ConnInfo> openPhoenixConnectionsMap;

    // Use tab instead of newline to make it easy to look for in Splunk
    static final String STACK_LINES_SEPARATOR = "\t";
    static final String BEGIN_CONN_INFO_VALS = "(";
    static final String CONN_INFO_VALS_SEPARATOR = ",";
    static final String END_CONN_INFO_VALS = ") ";

    private static final Logger LOGGER = LoggerFactory.getLogger(StackTraceMappingInfo.class);

    public StackTraceMappingInfo(int lfuCacheMaxCap, int maxOpenPhoenixConns,
            long connOpenTooLongWarnThresholdMs) {
        this.lfuCacheMaxCapacity = lfuCacheMaxCap;
        this.maxOpenPhoenixConnections = maxOpenPhoenixConns;
        this.stackTraceMap = new LoggingLFUCache<>(this.lfuCacheMaxCapacity);
        this.openPhoenixConnectionsMap = new ConcurrentHashMap<>(this.maxOpenPhoenixConnections);
        this.connectionOpenTooLongWarnThresholdMs = connOpenTooLongWarnThresholdMs;
    }

    @Override
    public void registerConnectionOpened(UUID phxConnUniqueID, long connOpenedTime) {
        Preconditions.checkNotNull(phxConnUniqueID, "Got null UUID for Phoenix Connection!");
        StackTraceElement[] st = Thread.currentThread().getStackTrace();
        int hashSt = getHashCodeForStackTrace(st);

        ConnInfo newConn = new ConnInfo(connOpenedTime, Thread.currentThread().getId(), hashSt);
        // This check needs to be thread-safe
        synchronized (this.openPhoenixConnectionsMapLock) {
            if (this.openPhoenixConnectionsMap.size() < this.maxOpenPhoenixConnections) {
                this.openPhoenixConnectionsMap.putIfAbsent(phxConnUniqueID, newConn);
            }
            // else skip adding to the map to limit memory usage. In practice, this map will not
            // grow beyond the QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS config since
            // we expect clearAll to be called every time the
            // SQLExceptionCode.NEW_CONNECTION_THROTTLED exception is thrown
        }
        synchronized (this.stackTraceMapLock) {
            // First do a get to possibly avoid recomputing the String stack trace if already cached
            String stString = this.stackTraceMap.get(hashSt);
            this.stackTraceMap.put(hashSt, stString == null ?
                    getCallerStackTrace(hashSt, st) : stString);
        }
    }

    @Override
    public void registerConnectionClosed(UUID phxConnUniqueID, long connClosedTime) {
        Preconditions.checkNotNull(phxConnUniqueID, "Got null UUID for Phoenix Connection!");
        ConnInfo connInfo = this.openPhoenixConnectionsMap.remove(phxConnUniqueID);
        if (connInfo == null) {
            // ignore this connection
            return;
        }
        long connKeptOpenMs = connClosedTime - connInfo.getConnOpenedTime();
        if (LOGGER.isDebugEnabled() && connKeptOpenMs > this.connectionOpenTooLongWarnThresholdMs) {
            logIfConnectionKeptOpenForTooLong(connInfo, connKeptOpenMs);
        }
    }

    @Override
    public void logInfoForAllOpenConnections() {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder logSb = new StringBuilder(String.valueOf(this.lfuCacheMaxCapacity));
            logSb.append(" most frequent stack traces found:\t");
            logSb.append(getMostFreqStackTraces(new StringBuilder()));
            LOGGER.debug(logSb.toString());

            logSb = new StringBuilder("Open connections info:\t");
            logSb.append(getOpenConnInfoMapString(new StringBuilder()));
            LOGGER.debug(logSb.toString());
        }
    }

    @Override
    public void clearAllState() {
        this.openPhoenixConnectionsMap.clear();
        synchronized (this.stackTraceMapLock) {
            this.stackTraceMap.clear();
        }
    }

    @VisibleForTesting
    ConcurrentHashMap<UUID, ConnInfo> getOpenPhoenixConnectionsMap() {
        return this.openPhoenixConnectionsMap;
    }

    @VisibleForTesting
    LoggingCache<Integer, String> getStackTraceMap() {
        synchronized (this.stackTraceMapLock) {
            return this.stackTraceMap;
        }
    }

    @VisibleForTesting
    int getLfuCacheMaxCapacity() {
        return this.lfuCacheMaxCapacity;
    }

    @VisibleForTesting
    int getMaxOpenPhoenixConnections() {
        return this.maxOpenPhoenixConnections;
    }

    @VisibleForTesting
    long getConnectionOpenTooLongWarnThresholdMs() {
        return this.connectionOpenTooLongWarnThresholdMs;
    }

    /*
    Represent Phoenix Connection information for extra logging
    */
    static class ConnInfo {
        private long connOpenedTime;
        private final long threadID;
        private final int hashStackTrace;

        public ConnInfo(long connOpenedTime, long threadID, int hashStackTrace) {
            this.connOpenedTime = connOpenedTime;
            this.threadID = threadID;
            this.hashStackTrace = hashStackTrace;
        }

        public long getConnOpenedTime()
        {
            return this.connOpenedTime;
        }

        public long getThreadID()
        {
            return this.threadID;
        }

        public int getHashStackTrace() {
            return this.hashStackTrace;
        }
    }

    /**
     * Get a String representation of the most frequent unique caller stack traces encountered.
     * @param sb StringBuilder object
     * @return A String with format defined in {@link LoggingLFUCache#getAllCachedKeyValuesAsString}
     */
    @VisibleForTesting
    String getMostFreqStackTraces(StringBuilder sb) {
        synchronized (this.stackTraceMapLock) {
            this.stackTraceMap.getAllCachedKeyValuesAsString(sb);
        }
        return sb.toString();
    }

    /**
     * Get a String representation of the open Phoenix connections.
     * @param sb StringBuilder object
     * @return A String with the following format:
     *
     * (CONN_INFO1) (CONN_INFO2) (CONN_INFO3) ..
     *
     * Where CONN_INFO is a tuple with format:
     * (<How long the connection was open in ms>,<Hash(Caller Stack Trace)>,<Caller thread ID>)
     */
    @VisibleForTesting
    String getOpenConnInfoMapString(StringBuilder sb) {
        long currentTime = System.currentTimeMillis();
        for (ConnInfo openConn : this.openPhoenixConnectionsMap.values()) {
            long elapsedTime = currentTime - openConn.getConnOpenedTime();
            sb.append(BEGIN_CONN_INFO_VALS);
            sb.append(elapsedTime);
            sb.append(CONN_INFO_VALS_SEPARATOR);
            sb.append(openConn.getHashStackTrace());
            sb.append(CONN_INFO_VALS_SEPARATOR);
            sb.append(openConn.getThreadID());
            sb.append(END_CONN_INFO_VALS);
        }
        return sb.toString();
    }

    /**
     * Get the caller stack trace as a string
     * @param hashSt hash of the stack trace
     * @param st Stack trace elements array
     * @return Caller stack trace elements separated by {@link this#STACK_LINES_SEPARATOR} as String
     */
    @VisibleForTesting
    String getCallerStackTrace(int hashSt, StackTraceElement[] st) {
        StringBuilder sb = new StringBuilder();
        // To avoid trailing separator
        if (st.length == 1) {
            sb.append(st[0]);
            return sb.toString();
        }
        for (StackTraceElement element : st) {
            String str = element.toString();
            sb.append(str);
            sb.append(STACK_LINES_SEPARATOR);
        }
        return sb.toString();
    }

    @VisibleForTesting
    void logIfConnectionKeptOpenForTooLong(ConnInfo connInfo, long connKeptOpenMs) {
        LOGGER.debug(String.format(
                "Closing connection that was kept open for too long: %d ms. H(ST)=%d, TID=%d",
                connKeptOpenMs, connInfo.getHashStackTrace(), connInfo.getThreadID()));
    }

    /**
     * Get a hashcode for the Stack trace
     * @param st Stack trace elements array
     * @return hashcode for array of stack trace elements
     */
    private static int getHashCodeForStackTrace(StackTraceElement[] st) {
        return Arrays.hashCode(st);
    }
}