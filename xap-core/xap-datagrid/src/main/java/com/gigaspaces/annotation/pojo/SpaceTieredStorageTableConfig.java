package com.gigaspaces.annotation.pojo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Define fields of a config table in tiered storage which represent cache rules that determine which entries will be stored in cache.
 *
 * </p>
 *
 * @author Sapir
 * @since 16.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpaceTieredStorageTableConfig {

    /**
     * sets the criteria field that hold a cache rule.
     */
    String criteria() default "";

    /**
     * Returns true if the type will be stored only in cache
     *
     * @return <code>true</code> if type is transient
     */
    boolean isTransient() default false;

    /**
     Set the time rule column which represents the timestamp
     @return the column name to be evaluated against the time rule period     * */
    String timeColumn() default "";


    String period() default "";



}
