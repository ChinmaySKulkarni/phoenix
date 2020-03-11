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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * An LFU cache implementation for extra logging that supports get and put in O(1) time
 * Note that frequency is only increased on puts, not gets.
 * Sample use case: Storing Phoenix connection caller stack traces
 * Note that this implementation is not thread-safe. The caller must ensure thread-safe usage.
 */
public class LoggingLFUCache<Key, Value> implements LoggingCache<Key, Value> {

    private final int maxCapacity;
    private final Map<Key, CachedNode<Value>> keyToCachedNodeMap;

    // The key is frequency and value is a LinkedHashSet of keys
    private final Map<Integer, LinkedHashSet<Key>> freqMap;
    private int currentMinFreq;

    static final String BEGIN_VALUES = "->[";
    static final String KEY_TOTAL_COUNT = "tc=";
    static final String KEY_TOTAL_COUNT_SEPARATOR = ";";
    static final String END_VALUES = "]\n";

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingLFUCache.class);

    public LoggingLFUCache(final int maxCapacity) {
        this.maxCapacity = maxCapacity;
        this.keyToCachedNodeMap = new HashMap<>(maxCapacity);
        // In the worst case, each key has a different frequency
        this.freqMap = new HashMap<>(maxCapacity);
        this.currentMinFreq = 0;
    }

    // O(1) operation
    @Override
    public Value get(Key key) {
        CachedNode<Value> cachedNode = this.keyToCachedNodeMap.get(key);
        if (cachedNode == null) {
            return null;
        }
        return cachedNode.getValue();
    }

    // O(1) operation
    @Override
    public void put(Key key, Value loggingObject) {
        if (this.maxCapacity == 0) {
            return;
        }
        // Evict LFU entry if we need to add a new element and the cache is already at max capacity
        if (!this.keyToCachedNodeMap.containsKey(key) && this.isFull()) {
            this.evict();
        }
        this.putElementInCache(key, loggingObject);
    }

    @Override
    public boolean isFull() {
        return this.size() == this.maxCapacity;
    }

    @Override public void clear() {
        this.keyToCachedNodeMap.clear();
        this.freqMap.clear();
        this.currentMinFreq = 0;
    }

    /**
     * The format of the returned String is:
     * Key->[tc=<frequency>;Value], for example:
     *
     * hash(ST1)->[tc=<frequency>;Entire stack trace string]
     * hash(ST2)->[tc=<frequency>;Entire stack trace string]
     * ...
     */
    @Override
    public String getAllCachedKeyValuesAsString(StringBuilder sb) {
        for (Map.Entry<Key, CachedNode<Value>> kv: this.keyToCachedNodeMap.entrySet()) {
            CachedNode<Value> cachedNode = kv.getValue();
            sb.append(kv.getKey());
            sb.append(BEGIN_VALUES);
            sb.append(KEY_TOTAL_COUNT);
            sb.append(cachedNode.getFrequency());
            sb.append(KEY_TOTAL_COUNT_SEPARATOR);
            sb.append(cachedNode.getValue());
            sb.append(END_VALUES);
        }
        return sb.toString();
    }

    @Override
    public int size() {
        return this.keyToCachedNodeMap.size();
    }

    /**
     * Remove the least frequently seen key and use LRU to break ties
     */
    private void evict() {
        LinkedHashSet<Key> minFreqBucket = this.freqMap.get(this.currentMinFreq);
        if (minFreqBucket == null || minFreqBucket.isEmpty()) {
            // Should never happen!
            LOGGER.warn("Eviction failed since minimum frequency bucket was found empty!");
            return;
        }
        Iterator<Key> itr = minFreqBucket.iterator();
        // Remove least recently seen key within the frequency bucket
        Key key = itr.next();
        itr.remove();
        this.keyToCachedNodeMap.remove(key);
        if (minFreqBucket.isEmpty()) {
            // Nothing left in this frequency bucket
            this.freqMap.remove(this.currentMinFreq);
            // Note that eviction is always followed by inserting a new element so
            // currentMinFreq will be set when inserting that new element
        }
    }

    private void putElementInCache(Key key, Value loggingObject) {
        CachedNode<Value> cachedNode = this.keyToCachedNodeMap.get(key);
        if (cachedNode == null) {
            // First time we are seeing this key, so we add it to the frequency 1 bucket
            cachedNode = new CachedNode<>(loggingObject);
            this.createNewFreqBucketIfRequiredAndAddKey(1, key);
            this.currentMinFreq = 1;
            this.keyToCachedNodeMap.put(key, cachedNode);
        } else {
            int bucketNum = cachedNode.getFrequency();
            LinkedHashSet<Key> freqBucket = this.freqMap.get(bucketNum);
            // Remove the key from this bucket and put it in the next higher frequency bucket
            freqBucket.remove(key);
            if (freqBucket.isEmpty()) {
                this.freqMap.remove(bucketNum);
                if (this.currentMinFreq == bucketNum) {
                    this.currentMinFreq++;
                }
            }
            cachedNode.incrementFrequency();
            cachedNode.setValue(loggingObject);
            this.createNewFreqBucketIfRequiredAndAddKey(bucketNum + 1, key);
        }
    }

    private void createNewFreqBucketIfRequiredAndAddKey(int bucketNum, Key key) {
        LinkedHashSet<Key> frequencyBucket = this.freqMap.get(bucketNum);
        if (frequencyBucket == null) {
            this.freqMap.put(bucketNum, new LinkedHashSet<Key>());
        }
        this.freqMap.get(bucketNum).add(key);
    }

    /**
     * Encapsulate the actual value to be stored inside this object which also counts the frequency
     * @param <Value> Object to be stored in the LFU cache
     */
    static class CachedNode<Value> {

        private Value value;
        private int frequency;

        public CachedNode(Value val) {
            this.value = val;
            this.frequency = 1;
        }

        public Value getValue() {
            return this.value;
        }

        public int getFrequency() {
            return this.frequency;
        }

        public void setValue(Value newValue) {
            this.value = newValue;
        }

        public void incrementFrequency() {
            this.frequency++;
        }
    }
}