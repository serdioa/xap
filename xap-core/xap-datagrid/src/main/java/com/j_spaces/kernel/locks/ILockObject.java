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

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 6.6 the basic interface for a lock object. it can be the locked subject itself or a
 * representing object for eviction based locking
 */
public interface ILockObject {

    /**
     * returns true if the lock object is the subject itself (i.e. entry or template) or a
     * representing object
     *
     * @return true if is the subject itself
     */
    boolean isLockSubject();

    /**
     * get the uid for the subject
     *
     * @return the uid
     */
    String getUID();

    /**
     * @return if this subject need to be locked by its uid or by itself.
     */
    default LockSubjectType getLockSubjectType() {
        return LockSubjectType.ENTRY;
    }
}
