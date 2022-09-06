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
