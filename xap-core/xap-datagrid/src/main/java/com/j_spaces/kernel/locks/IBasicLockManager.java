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
 * @author Yechiel Fefer
 * @version 1.0
 * @since 6.5 contains the basic methods for acuring a lock object- i.e. an object that will be
 * locked in order to lock the entity (entry or template) represented by it
 */
public interface IBasicLockManager<T extends ILockObject> {

    /**
     * based on subject, return a lock object in order to lock the represented subject.
     * <p>
     * If the subject is part of evictable from space (like lru)
     * or evicatble from cache (like in TieredStorage with cache rule)
     * we use the subject itself, otherwise we use per-logical subject a different object.
     * <p>
     * Note - template and transient entry are not evictable.
     *
     * @return the lock object
     */
    ILockObject getLockObject(T subject);

    /**
     * based only on subject's uid, return a lock object in order to lock the represented subject
     * this method is relevant only for evictable objects
     *
     * @return the lock object
     */
    ILockObject getLockObject(String subjectUid);

    /**
     * free the lock object - no more needed by this thread
     *
     * @param lockObject the lock object
     */
    void freeLockObject(ILockObject lockObject);

    /**
     * true if the lock object is the subject itself (i.e. entry or template), false if it is representing object.
     * Note - this method is for entries only!.
     *
     * @return true if is the subject itself
     */
    boolean isEntryLocksItsSelf(T entry);

}
