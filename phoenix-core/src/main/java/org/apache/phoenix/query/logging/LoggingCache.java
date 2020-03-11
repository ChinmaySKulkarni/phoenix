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

/**
 * This interface defines the basic contract for caching Key-Values for extra logging
 */
public interface LoggingCache<Key,Value> {

    /**
     * Get the logging object associated with a given Key if it is present in the cache
     * @param key Key
     * @return Logging object as value
     */
    Value get(Key key);

    /**
     *  Store the mapping for Key -> logging object value in the cache with possible eviction
     *  based on the cache eviction policy
     * @param key Key
     * @param loggingObject Logging object
     */
    void put(Key key, Value loggingObject);

    /**
     * Check if the cache is at its maximum capacity
     * @return true/false
     */
    boolean isFull();

    /**
     * Remove all mappings from the cache
     */
    void clear();

    /**
     * Get a String representation of all the key values in the cache
     * @param sb String builder object
     * @return String
     */
    String getAllCachedKeyValuesAsString(StringBuilder sb);

    /**
     * Get the number of elements in the cache
     * @return number of elements in the cache
     */
    int size();
}