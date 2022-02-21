package com.gigaspaces.internal.server.metadata;

import com.gigaspaces.metrics.LongCounter;

public class TypeCounters {
    /* consider removing total read counter
    total read counter is not ram+disk read counters, for two reasons:
    can't be disk only, since disk doesn't include transient
    ram+disk includes duplicates of read from both */
    private final LongCounter totalReadCounter;
    private final LongCounter ramReadAccessCounter;
    private final LongCounter diskReadAccessCounter;
    private final LongCounter diskModifyCounter;
    private final LongCounter diskEntriesCounter;
    private final LongCounter ramEntriesCounter;
    private boolean disabled;

    public TypeCounters() {
        totalReadCounter = new LongCounter();
        ramReadAccessCounter = new LongCounter();
        diskReadAccessCounter = new LongCounter();
        diskModifyCounter = new LongCounter();
        diskEntriesCounter = new LongCounter();
        ramEntriesCounter = new LongCounter();
        disabled = false;
    }

    public TypeCounters(TypeCounters other) {
        totalReadCounter = other.totalReadCounter;
        ramReadAccessCounter = other.ramReadAccessCounter;
        diskReadAccessCounter = other.diskReadAccessCounter;
        diskModifyCounter = other.diskModifyCounter;
        diskEntriesCounter = other.diskEntriesCounter;
        ramEntriesCounter = other.ramEntriesCounter;
    }

    public LongCounter getTotalReadCounter() {
        return totalReadCounter;
    }

    public void incTotalReadCounter() {
        if(!disabled){
            totalReadCounter.inc();
        }
    }

    public LongCounter getRamReadAccessCounter() {
        return ramReadAccessCounter;
    }

    public void incRamReadAccessCounter() {
        if(!disabled){
            ramReadAccessCounter.inc();
        }
    }

    public LongCounter getDiskReadAccessCounter() {
        return diskReadAccessCounter;
    }

    public void incDiskReadCounter() {
        if(!disabled){
            diskReadAccessCounter.inc();
        }
    }

    public LongCounter getDiskModifyCounter() {
        return diskModifyCounter;
    }

    public void incDiskModifyCounter() {
        if(!disabled) {
            diskModifyCounter.inc();
        }
    }

    public LongCounter getDiskEntriesCounter() {
        return diskEntriesCounter;
    }

    public void incDiskEntriesCounter() {
        if(!disabled) {
            diskEntriesCounter.inc();
        }
    }

    public void decDiskEntriesCounter() {
        if(!disabled){
            diskEntriesCounter.dec();
        }
    }

    public void setDiskEntriesCounter(int count) {
        if (!disabled) {
            diskEntriesCounter.inc(count);
        }
    }

    public LongCounter getRamEntriesCounter() {
        return ramEntriesCounter;
    }

    public void incRamEntriesCounter() {
        if (!disabled) {
            ramEntriesCounter.inc();
        }
    }

    public void decRamEntriesCounter() {
        if (!disabled) {
            ramEntriesCounter.dec();
        }
    }

    public void disable() {
        disabled = true;
    }
}
