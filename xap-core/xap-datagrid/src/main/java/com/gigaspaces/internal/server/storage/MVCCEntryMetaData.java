package com.gigaspaces.internal.server.storage;

import java.io.Serializable;

public class MVCCEntryMetaData implements Serializable {

    private static final long serialVersionUID = 8863674948026337024L;
    private long createdGeneration;
    private long overrideGeneration;
    private boolean isOverridingAnother;
    private boolean isLogicallyDeleted;

    public MVCCEntryMetaData() {
    }

    public long getCreatedGeneration() {
        return createdGeneration;
    }

    public void setCreatedGeneration(long createdGeneration) {
        this.createdGeneration = createdGeneration;
    }

    public long getOverrideGeneration() {
        return overrideGeneration;
    }

    public void setOverrideGeneration(long overrideGeneration) {
        this.overrideGeneration = overrideGeneration;
    }

    public boolean isOverridingAnother() {
        return isOverridingAnother;
    }

    public void setOverridingAnother(boolean overridingAnother) {
        isOverridingAnother = overridingAnother;
    }

    public boolean isLogicallyDeleted() {
        return isLogicallyDeleted;
    }

    public void setLogicallyDeleted(boolean logicallyDeleted) {
        isLogicallyDeleted = logicallyDeleted;
    }
}
