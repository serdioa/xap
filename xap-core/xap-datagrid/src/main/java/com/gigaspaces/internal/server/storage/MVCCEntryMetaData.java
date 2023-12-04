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
package com.gigaspaces.internal.server.storage;

import java.io.Serializable;

public class MVCCEntryMetaData implements Serializable {

    private static final long serialVersionUID = 8863674948026337024L;
    private long committedGeneration;
    private long overrideGeneration;
    private boolean isOverridingAnother;
    private boolean isLogicallyDeleted;
    private int version;

    public MVCCEntryMetaData() {
    }

    public long getCommittedGeneration() {
        return committedGeneration;
    }

    public void setCommittedGeneration(long committedGeneration) {
        this.committedGeneration = committedGeneration;
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "MVCCEntryMetaData{" +
                "committedGeneration=" + committedGeneration +
                ", overrideGeneration=" + overrideGeneration +
                ", isOverridingAnother=" + isOverridingAnother +
                ", isLogicallyDeleted=" + isLogicallyDeleted +
                ", version=" + version +
                '}';
    }
}
