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

//
package com.j_spaces.kernel.locks;

/**
 * locks The resident part of entry that resides in-heap
 *
 * @author yechiel
 * @since 9.8
 */

@com.gigaspaces.api.InternalApi
public class BlobStoreLockManager<T extends ISelfLockingSubject>
        implements IBasicLockManager<T> {

    @Override
    public ILockObject getLockObject(T subject) {
        return subject.getExternalLockObject() != null ? subject.getExternalLockObject() : subject;
    }

    @Override
    public ILockObject getLockObject(String subjectUid) {
        throw new RuntimeException("BlobStoreLockManager::getLockObject based on uid is not supported");
    }

    @Override
    public void freeLockObject(ILockObject lockObject) {
        return;
    }

    @Override
    public boolean isEntryLocksItsSelf(T entry) {
        // In BlobStore 'EntryLocksItsSelf' is some part of the Entry used for the locking,
        // Therefore always true, and not -
        // return entry.getExternalLockObject() == null;
        return true;
    }


}
