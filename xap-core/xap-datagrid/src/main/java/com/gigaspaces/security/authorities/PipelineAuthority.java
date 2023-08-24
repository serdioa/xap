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
package com.gigaspaces.security.authorities;

public class PipelineAuthority implements InternalAuthority {

    /**
     * Defines pipeline privileges
     */
    public enum PipelinePrivilege implements Privilege {
        CREATE,
        START_STOP,
        EDIT,
        DELETE;

        @Override
        public String toString() {
            switch (this) {
                case CREATE:
                    return "Create pipeline";
                case START_STOP:
                    return "Start/Stop pipeline";
                case EDIT:
                    return "Edit pipeline (tables)";
                case DELETE:
                    return "Delete pipeline";
                default:
                    return super.toString();
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private final PipelinePrivilege pipelinePrivilege;

    public PipelineAuthority(PipelinePrivilege pipelinePrivilege) {
        this.pipelinePrivilege = pipelinePrivilege;
    }

    public static PipelineAuthority valueOf(String authority) {
        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!PipelinePrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Authority name in: " + authority);
        }

        PipelinePrivilege systemPrivilege = PipelinePrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        return new PipelineAuthority(systemPrivilege);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return pipelinePrivilege.getClass().getSimpleName() + Constants.DELIM + pipelinePrivilege.name();
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((getAuthority() == null) ? 0 : getAuthority().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PipelineAuthority other = (PipelineAuthority) obj;
        if (getAuthority() == null) {
            if (other.getAuthority() != null)
                return false;
        } else if (!getAuthority().equals(other.getAuthority()))
            return false;
        return true;
    }


    /*
     * @see com.gigaspaces.security.authorities.InternalAuthority#getMappingKey()
     */
    public Privilege getPrivilege() {
        return pipelinePrivilege;
    }
}
