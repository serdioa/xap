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


package com.j_spaces.kernel.locks;

import com.gigaspaces.internal.server.space.SpaceConfigReader;

import static com.j_spaces.core.Constants.TieredStorage.CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.TieredStorage.CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_PROP;


@com.gigaspaces.api.InternalApi
public class TieredStorageLockManager<T extends ILockObject>
        implements IBasicLockManager<T> {
    private static class LockObject implements ILockObject {

        @Override
        public boolean isLockSubject() {
            return false;
        }

        @Override
        public String getUID() {
            return null;
        }
    }

    private final LockObject[] _locks;

    public TieredStorageLockManager(SpaceConfigReader configReader) {
        int size = configReader.getIntSpaceProperty(CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_PROP,
                CACHE_MANAGER_TIERED_STORAGE_LOCKS_SIZE_DEFAULT);

        _locks = new LockObject[size];
        for (int i = 0; i < size; i++)
            _locks[i] = new LockObject();
    }

    @Override
    public ILockObject getLockObject(T subject) {
        if (subject.getLockSubjectType() != LockSubjectType.ENTRY)
            return subject; //return the subject when entry is a template or transient
        return getLockObject_impl(subject.getUID());
    }

    @Override
    public ILockObject getLockObject(String subjectUid) {
        throw new RuntimeException("TieredStorageLockManager::getLockObject based on uid is not supported");
    }

    private ILockObject getLockObject_impl(String subjectUid) {
        return _locks[Math.abs(subjectUid.hashCode() % _locks.length)];
    }

    @Override
    public void freeLockObject(ILockObject lockObject) {
        return;
    }

    @Override
    public boolean isEntryLocksItsSelf(T entry) {
        return entry.getLockSubjectType() != LockSubjectType.ENTRY;
    }
}
