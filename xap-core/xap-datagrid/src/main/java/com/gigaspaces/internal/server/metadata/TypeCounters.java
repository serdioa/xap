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
package com.gigaspaces.internal.server.metadata;

import com.gigaspaces.metrics.LongCounter;

public class TypeCounters {
    /* consider removing total read counter
    total read counter is not ram+disk read counters, for two reasons:
    can't be disk only, since disk doesn't include transient
    ram+disk includes duplicates of read from both */
    private final LongCounter totalReadCounter;

    //Tiered Storage Counters
    private final LongCounter ramReadAccessCounter;
    private final LongCounter diskReadAccessCounter;
    private final LongCounter diskModifyCounter;
    private final LongCounter diskEntriesCounter;
    private final LongCounter ramEntriesCounter;

    //true by default - false when tiered storage metrics are disabled
    private boolean enabledTieredStorageMetrics = true;

    public TypeCounters() {
        totalReadCounter = new LongCounter();
        ramReadAccessCounter = new LongCounter();
        diskReadAccessCounter = new LongCounter();
        diskModifyCounter = new LongCounter();
        diskEntriesCounter = new LongCounter();
        ramEntriesCounter = new LongCounter();
    }

    public TypeCounters(TypeCounters other) {
        totalReadCounter = other.totalReadCounter;
        ramReadAccessCounter = other.ramReadAccessCounter;
        diskReadAccessCounter = other.diskReadAccessCounter;
        diskModifyCounter = other.diskModifyCounter;
        diskEntriesCounter = other.diskEntriesCounter;
        ramEntriesCounter = other.ramEntriesCounter;
        enabledTieredStorageMetrics = other.enabledTieredStorageMetrics;
    }

    public LongCounter getTotalReadCounter() {
        return totalReadCounter;
    }

    public void incTotalReadCounter() {
        totalReadCounter.inc();
    }

    public LongCounter getRamReadAccessCounter() {
        return ramReadAccessCounter;
    }

    public void incRamReadAccessCounter() {
        if(enabledTieredStorageMetrics){
            ramReadAccessCounter.inc();
        }
    }

    public LongCounter getDiskReadAccessCounter() {
        return diskReadAccessCounter;
    }

    public void incDiskReadCounter() {
        if(enabledTieredStorageMetrics){
            diskReadAccessCounter.inc();
        }
    }

    public LongCounter getDiskModifyCounter() {
        return diskModifyCounter;
    }

    public void incDiskModifyCounter() {
        if(enabledTieredStorageMetrics) {
            diskModifyCounter.inc();
        }
    }

    public LongCounter getDiskEntriesCounter() {
        return diskEntriesCounter;
    }

    public void incDiskEntriesCounter() {
        if(enabledTieredStorageMetrics) {
            diskEntriesCounter.inc();
        }
    }

    public void decDiskEntriesCounter() {
        if(enabledTieredStorageMetrics){
            diskEntriesCounter.dec();
        }
    }

    public void setDiskEntriesCounter(int count) {
        if (enabledTieredStorageMetrics) {
            diskEntriesCounter.inc(count);
        }
    }

    public LongCounter getRamEntriesCounter() {
        return ramEntriesCounter;
    }

    public void incRamEntriesCounter() {
        if (enabledTieredStorageMetrics) {
            ramEntriesCounter.inc();
        }
    }

    public void decRamEntriesCounter() {
        if (enabledTieredStorageMetrics) {
            ramEntriesCounter.dec();
        }
    }

    public void disableTieredStorageMetrics() {
        enabledTieredStorageMetrics = false;
    }

    public boolean isEnabledTieredStorageMetrics(){
        return enabledTieredStorageMetrics;
    }

}
