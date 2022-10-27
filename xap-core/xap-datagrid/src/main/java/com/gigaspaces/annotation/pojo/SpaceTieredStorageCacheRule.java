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
package com.gigaspaces.annotation.pojo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Define cache rules that determine which entries will be available in RAM for faster access.
 * Entries that don't match the criteria/time-rule, will be accessed from disk.
 * </p>
 *
 * @author Sapir
 * @since 16.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpaceTieredStorageCacheRule {

    /**
     * @return The criteria field that holds a cache rule.
     */
    String criteria() default "";

    /**
     * @return The name of the timestamp field to be evaluated against the {@link #period()}.
     */
    String timeColumn() default "";

    /**
     * Set the {@link java.time.Duration} period to keep in RAM.
     *
     * @return a text string such as {@code PnDTnHnMn.nS} representing a {@code Duration}
     */
    String period() default "";
}
