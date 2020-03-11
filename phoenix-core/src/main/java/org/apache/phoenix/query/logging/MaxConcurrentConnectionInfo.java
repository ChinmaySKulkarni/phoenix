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

import java.util.UUID;

/**
 * This interface defines the contract for storing information about Phoenix connections
 * for debugging client-side issues like
 * {@link org.apache.phoenix.exception.SQLExceptionCode#NEW_CONNECTION_THROTTLED}
 */
public interface MaxConcurrentConnectionInfo {

    /**
     * Store information for the newly opened Phoenix connection. This is expected to be called
     * when the user creates a new Phoenix connection
     * @param phxConnUniqueID UUID uniquely identifying the Phoenix connection
     * @param connOpenedTime Time at which the connection was opened
     */
    void registerConnectionOpened(UUID phxConnUniqueID, long connOpenedTime);

    /**
     * Store information to indicate that this Phoenix connection was closed. This is expected to
     * be called when the user closes the Phoenix connection
     * @param phxConnUniqueID UUID uniquely identifying the Phoenix connection
     * @param connClosedTime Time at which the connection was closed
     */
    void registerConnectionClosed(UUID phxConnUniqueID, long connClosedTime);

    /**
     * Log information for all stored open Phoenix connections
     */
    void logInfoForAllOpenConnections();

    /**
     * Clear all information stored across all Phoenix connections
     */
    void clearAllState();
}