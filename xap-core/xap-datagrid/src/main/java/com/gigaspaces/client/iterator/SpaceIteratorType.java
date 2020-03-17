/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.client.iterator;

/**
 * Enum of {@link SpaceIterator} implementation types
 * @Author Alon Shoham
 * @Since 15.2.0
 */
public enum SpaceIteratorType {
    /**
     * This type creates an iterator on the server side in each partition (similar to database cursors)
     * On iterator init, an entries batch is fetched from each partition and stored on client side.
     * With each partition batch consumption, the next batch is fetched in the background from that specific partition
     * Advantages:
     *  1. Small memory footprint on client side
     *  2. Small workload on space: single batch is fetched from each partition on any given time
     * Disadvantages:
     *  1. Intolerant for primary space failure in a partition:
     *      After primary space failure, entries will not be fetched from that partition. Iterator will continue to function properly otherwise.
     */
    CURSOR,
    /**
     * This type is an iterable wrapper over gigaSpace.readMultiple
     * On init, all template matching entries UUIDs are stored on client side.
     * On iteration batches of entries are fetched from space, utilizing the stored UUIDs
     * Advantages:
     *  1. Tolerant to primary space failure in any partition
     * Disadvantages:
     *  1. Large memory footprint on client side
     *  2. Large workload on space: all template matching entries need to be gathered on init
     *
     */
    PREFETCH_UIDS
}
