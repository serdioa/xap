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

/*
 * Title:		 RelativeTime.java
 * Description: High Precision timer implementation of ITimeProvider interface.
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 12/12/2005
 * @since		 5.0EAG Build#1315
 */
package com.gigaspaces.time;

/**
 * {@linkplain ITimeProvider ITimeProvider} implementation specifying a precise timer, which is not
 * influenced by motions of the wall-clock time.
 */
public final class RelativeTime implements ITimeProvider {
    final static long TO_MILLISECONDS = 1_000_000;

    /**
     * Returns a RelativeTime instance based on {@code System.nanoTime()} precision.
     */
    public RelativeTime() {
    }

    /**
     * Returns the current value of the most precise available system timer, in milliseconds. This
     * method is used to measure elapsed time and is not related to any other notion of system or
     * wall-clock time.
     *
     * @return The current value of the system timer, in milliseconds.
     */
    public long timeMillis() {
        return System.nanoTime() / TO_MILLISECONDS;
    }

    @Override
    public boolean isRelative() {
        return true;
    }
}
