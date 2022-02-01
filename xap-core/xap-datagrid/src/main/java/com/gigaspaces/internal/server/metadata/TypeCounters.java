package com.gigaspaces.internal.server.metadata;

import com.gigaspaces.metrics.LongCounter;

public class TypeCounters {
    //todo: consider removing total read counter
    private final LongCounter totalReadCounter;
    private final LongCounter ramReadAccessCounter;
    private final LongCounter diskReadAccessCounter;
    private final LongCounter diskModifyCounter;
    private final LongCounter diskEntriesCounter;
    private final LongCounter ramEntriesCounter;

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
    }

    public LongCounter getTotalReadCounter() {
        return totalReadCounter;
    }

    public void incTotalReadCounter(){
        totalReadCounter.inc();
    }

    public LongCounter getRamReadAccessCounter() {
        return ramReadAccessCounter;
    }

    public void incRamReadAccessCounter(){
        ramReadAccessCounter.inc();
    }

    public LongCounter getDiskReadAccessCounter() {
        return diskReadAccessCounter;
    }

    public void incDiskReadCounter(){
        diskReadAccessCounter.inc();
    }

    public LongCounter getDiskModifyCounter() {
        return diskModifyCounter;
    }

    public void incDiskModifyCounter(){
        diskModifyCounter.inc();
    }

    public LongCounter getDiskEntriesCounter() {
        return diskEntriesCounter;
    }

    public void incDiskEntriesCounter(){
        diskEntriesCounter.inc();
    }

    public void decDiskEntriesCounter() {
        diskEntriesCounter.dec();
    }

    public void setDiskEntriesCounter(int count){ diskEntriesCounter.inc(count);}

    public LongCounter getRamEntriesCounter() {
        return ramEntriesCounter;
    }

    public void incRamEntriesCounter(){
        ramEntriesCounter.inc();
    }

    public void decRamEntriesCounter() {
        ramEntriesCounter.dec();
    }
}
