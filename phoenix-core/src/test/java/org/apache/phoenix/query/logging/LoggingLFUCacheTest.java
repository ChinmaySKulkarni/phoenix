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
import java.util.regex.Pattern;

import static org.apache.phoenix.query.logging.LoggingLFUCache.BEGIN_VALUES;
import static org.apache.phoenix.query.logging.LoggingLFUCache.END_VALUES;
import static org.apache.phoenix.query.logging.LoggingLFUCache.KEY_TOTAL_COUNT;
import static org.apache.phoenix.query.logging.LoggingLFUCache.KEY_TOTAL_COUNT_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoggingLFUCacheTest {

    private static final String ESC_BEG_VALS = Pattern.quote(BEGIN_VALUES);
    private static final String ESC_END_VALS = Pattern.quote(END_VALUES);
    private static final String ESC_KEY_TOTAL_COUNT = Pattern.quote(KEY_TOTAL_COUNT);
    private static final String ESC_KEY_TOTAL_COUNT_SEP = Pattern.quote(KEY_TOTAL_COUNT_SEPARATOR);

    @Test
    public void testZeroMaxCapacityNoPutsGoThrough() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(0);
        lfuCache.put(1,"1");
        assertNull(lfuCache.get(1));
    }

    @Test
    public void testClear() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(2);
        lfuCache.put(1, "1");
        lfuCache.put(2, "2");
        assertEquals("1", lfuCache.get(1));
        lfuCache.clear();
        assertNull(lfuCache.get(1));
        assertNull(lfuCache.get(2));
        assertEquals(0, lfuCache.size());
    }

    @Test
    public void testGetWithoutPutGivesNull() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(1);
        assertNull(lfuCache.get(1));
        lfuCache.put(1, "1");
        assertEquals("1", lfuCache.get(1));
    }

    @Test
    public void testLFUKeysRemovedFromCache() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(2);
        int mostFreqKey = 10;
        int secondMostFreqKey = 20;
        int leastFreqKey = 30;
        String secondMostFreqKeyLatestValue = String.valueOf(secondMostFreqKey + 1);
        String mostFreqKeyLatestValue = String.valueOf(mostFreqKey + 2);

        lfuCache.put(leastFreqKey, String.valueOf(leastFreqKey));
        lfuCache.put(secondMostFreqKey, String.valueOf(secondMostFreqKey));
        lfuCache.put(secondMostFreqKey, secondMostFreqKeyLatestValue);
        lfuCache.put(mostFreqKey, String.valueOf(mostFreqKey));
        lfuCache.put(mostFreqKey, String.valueOf(mostFreqKey + 1));
        // This put should evict the first key that was added
        lfuCache.put(mostFreqKey, mostFreqKeyLatestValue);

        assertNull(lfuCache.get(leastFreqKey));
        // Make sure the value reflects most recent put for a given key
        assertEquals(secondMostFreqKeyLatestValue, lfuCache.get(secondMostFreqKey));
        assertEquals(mostFreqKeyLatestValue, lfuCache.get(mostFreqKey));
        assertEquals(2,lfuCache.size());
        assertTrue(lfuCache.isFull());
    }

    @Test
    public void testBreakTiesWithLRU() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(2);
        int key1 = 1;
        int key2 = 2;
        int key3 = 3;
        String val1 = String.valueOf(key1);
        String val2 = String.valueOf(key2);
        int NUM_ENTRIES = 3;

        for (int i = 0; i < NUM_ENTRIES; i++) {
            lfuCache.put(key1, val1);
        }
        for (int i = 0; i < NUM_ENTRIES; i++) {
            lfuCache.put(key2, val2);
        }
        // Though key1 and key2 have the same frequency, key1 will be evicted since it had the
        // least recent put
        lfuCache.put(key3, String.valueOf(key3));
        assertNull(lfuCache.get(key1));
        assertEquals(val2, lfuCache.get(key2));
    }

    @Test
    public void testGetAllCachedKeyValuesAsString() {
        LoggingLFUCache<Integer, String> lfuCache = new LoggingLFUCache<>(5);
        int key1 = 1;
        int key2 = 2;
        String val1 = String.valueOf(key1);
        String latestVal1 = "latestVal1";
        String val2 = String.valueOf(key2);
        String latestVal2 = "latestVal2";
        for (int i = 0; i < 10; i++) {
            lfuCache.put(key1, val1);
        }
        for (int i = 0; i < 7; i++) {
            lfuCache.put(key2, val2);
        }
        lfuCache.put(key1, latestVal1);
        lfuCache.put(key2, latestVal2);

        StringBuilder sb = new StringBuilder();
        String cacheMapStr = lfuCache.getAllCachedKeyValuesAsString(sb);

        String[] kvStrings = cacheMapStr.split(ESC_END_VALS);
        assertEquals(2, kvStrings.length);

        // Find which one corresponds to the first key
        String firstKeyValStr = kvStrings[0];
        String secondKeyValStr = kvStrings[1];
        if (Integer.parseInt(kvStrings[0].split(ESC_BEG_VALS)[0]) == key2) {
            firstKeyValStr = kvStrings[1];
            secondKeyValStr = kvStrings[0];
        }
        firstKeyValStr = firstKeyValStr.split(ESC_BEG_VALS)[1];
        secondKeyValStr = secondKeyValStr.split(ESC_BEG_VALS)[1];

        // Check the mapping has the latest values
        String[] firstKeyTotalCountValArr = firstKeyValStr.split(ESC_KEY_TOTAL_COUNT_SEP);
        String[] secondKeyTotalCountValArr = secondKeyValStr.split(ESC_KEY_TOTAL_COUNT_SEP);
        assertEquals(latestVal1, firstKeyTotalCountValArr[1]);
        assertEquals(latestVal2, secondKeyTotalCountValArr[1]);

        // Check that total counts of keys are correct
        assertEquals("11", firstKeyTotalCountValArr[0].split(ESC_KEY_TOTAL_COUNT)[1]);
        assertEquals("8", secondKeyTotalCountValArr[0].split(ESC_KEY_TOTAL_COUNT)[1]);
    }
}