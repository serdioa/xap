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

package com.gigaspaces.internal.utils.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@com.gigaspaces.api.InternalApi
public class ListCountDownLatch<T> extends CountDownLatch {
    private final List<T> _list;
    private final Object addLock = new Object();

    public ListCountDownLatch() {
        this(1);
    }

    public ListCountDownLatch(int count) {
        super(count);
        _list = new ArrayList<>(count);
    }


    public List<T> countDown(T e) {
        synchronized (addLock){
            _list.add(e);
            countDown();
            if(getCount() == 0){
                return _list;
            } else{
                return null;
            }
        }
    }

    public List<T> waitForResult()
            throws InterruptedException {
        super.await();
        return _list;
    }

    public List<T> waitForResult(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        if (super.await(timeout, unit))
            return _list;

        throw new TimeoutException("Operation timed out after " + timeout + " " + unit.name().toLowerCase());
    }
}
