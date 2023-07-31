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


package com.gigaspaces.security;

import com.gigaspaces.security.authorities.Constants;
import com.gigaspaces.security.authorities.GridAuthority;
import com.gigaspaces.security.authorities.GridAuthority.GridPrivilege;
import com.gigaspaces.security.authorities.MonitorAuthority;
import com.gigaspaces.security.authorities.MonitorAuthority.MonitorPrivilege;
import com.gigaspaces.security.authorities.PipelineAuthority;
import com.gigaspaces.security.authorities.PipelineAuthority.PipelinePrivilege;
import com.gigaspaces.security.authorities.Privilege;
import com.gigaspaces.security.authorities.RoleAuthority;
import com.gigaspaces.security.authorities.RoleAuthority.RolePrivilege;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.authorities.SystemAuthority;
import com.gigaspaces.security.authorities.SystemAuthority.SystemPrivilege;

/**
 * A factory for creating an {@link Authority} instance back from its String representation returned
 * by {@link Authority#getAuthority()}
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public final class AuthorityFactory {

    /**
     * Creates an {@link Authority} instance out of its String representation {@link
     * Authority#getAuthority()}.
     *
     * @param authority An authority String representation.
     * @return An authority instance.
     * @deprecated since 15.5 - use {@link #valueOf(String)} instead.
     */
    public static Authority create(String authority) {
        return valueOf(authority);
    }

    /**
     * Creates an {@link Authority} instance out its short (or long) name String representation.
     * Calls {@link #create(String)} if short representation is not matched.
     * @param authority An authority String representation
     * @return An authority instance
     * @since 15.5
     */
    public static Authority valueOf(String authority) {
        switch (authority) {
            case "MANAGE_GRID":  return valueOf(GridPrivilege.MANAGE_GRID);
            case "MANAGE_PU":    return valueOf(GridPrivilege.MANAGE_PU);
            case "PROVISION_PU": return valueOf(GridPrivilege.PROVISION_PU);
            case "MONITOR_JVM":  return valueOf(MonitorPrivilege.MONITOR_JVM);
            case "MONITOR_PU":   return valueOf(MonitorPrivilege.MONITOR_PU);
            case "SPACE_ALTER":  return valueOf(SpacePrivilege.ALTER);
            case "SPACE_CREATE":   return valueOf(SpacePrivilege.CREATE);
            case "SPACE_EXECUTE":   return valueOf(SpacePrivilege.EXECUTE);
            case "SPACE_READ":   return valueOf(SpacePrivilege.READ);
            case "SPACE_WRITE":   return valueOf(SpacePrivilege.WRITE);
            case "SPACE_TAKE":   return valueOf(SpacePrivilege.TAKE);
        }
        if (authority.startsWith("SPACE_")) {
            return _valueOfWithClassPrefix("SpacePrivilege " + authority.substring("SPACE_".length()));
        }
        return _valueOfWithClassPrefix(authority);
    }

    /**
     * Internal API - backwards full syntax support including privilege class name.
     * Used to be the implementation of {@link #create(String)}.
     *
     * @param authority the authority String with Class Prefix
     * @return an authority instance
     * @since 15.5
     */
    private static Authority _valueOfWithClassPrefix(String authority) {
        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }
        String privilege = split[Constants.PRIVILEGE_NAME_POS];
        if (RolePrivilege.class.getSimpleName().equals(privilege)) {
            return RoleAuthority.valueOf(authority);
        } else if (SystemPrivilege.class.getSimpleName().equals(privilege)) {
            return SystemAuthority.valueOf(authority);
        } else if (SpacePrivilege.class.getSimpleName().equals(privilege)) {
            return SpaceAuthority.valueOf(authority);
        } else if (GridPrivilege.class.getSimpleName().equals(privilege)) {
            return GridAuthority.valueOf(authority);
        } else if (MonitorPrivilege.class.getSimpleName().equals(privilege)) {
            return MonitorAuthority.valueOf(authority);
        } else if (PipelineAuthority.PipelinePrivilege.class.getSimpleName().equals(privilege)) {
            return PipelineAuthority.valueOf(authority);
        }

        throw new IllegalArgumentException("Unknown authority type; Could not create an Authority from: " + authority);
    }

    /**
     * Converts Privilege to Authority
     * @param privilege
     * @return an Authority encapsulating this privilege
     * @since 15.5
     */
    public static Authority valueOf(Privilege privilege) {
        if (privilege instanceof GridPrivilege) {
            return new GridAuthority((GridPrivilege) privilege);
        } else if (privilege instanceof MonitorPrivilege) {
            return new MonitorAuthority(((MonitorPrivilege) privilege));
        } else if (privilege instanceof SpacePrivilege) {
            return new SpaceAuthority(((SpacePrivilege) privilege));
        } else if (privilege instanceof SystemPrivilege) {
            return new SystemAuthority((SystemPrivilege) privilege);
        } else if (privilege instanceof PipelinePrivilege) {
            return new PipelineAuthority((PipelinePrivilege) privilege);
        }

        throw new IllegalArgumentException("unknown privilege " + privilege);
    }
}
