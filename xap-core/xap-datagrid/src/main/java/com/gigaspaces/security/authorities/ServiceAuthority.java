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

public class ServiceAuthority implements InternalAuthority {

    /**
     * Defines service privileges
     */
    public enum ServicePrivilege implements Privilege {
        /**
         * create/configure/delete service
         */
        //todo activate in scope UI part will add validation
        //CONFIGURE_SERVICE,
        /**
         * deploy/undeploy service
         */
        MANAGE_SERVICE;

        @Override
        public String toString() {
            switch (this) {
                /*case CONFIGURE_SERVICE:
                    return "Create/Configure/Delete Service";*/
                case MANAGE_SERVICE:
                    return "Deploy/Undeploy Service";
                default:
                    return super.toString();
            }
        }
    }

    private static final long serialVersionUID = 4934473878982940833L;
    private final ServiceAuthority.ServicePrivilege servicePrivilege;
    private final ServiceFilter filter;

    public ServiceAuthority(ServiceAuthority.ServicePrivilege servicePrivilege) {
        this(servicePrivilege, null);
    }

    /**
     * An authority with the specified privilege.
     *
     * @param servicePrivilege granted privilege
     * @param filter         a filter on the specified privilege.
     */
    public ServiceAuthority(ServiceAuthority.ServicePrivilege servicePrivilege, ServiceFilter filter) {
        this.servicePrivilege = servicePrivilege;
        this.filter = filter;
    }

    public static ServiceAuthority valueOf(String authority) {
        if (authority == null) {
            throw new IllegalArgumentException("Illegal Authority format: null");
        }

        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!ServiceAuthority.ServicePrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Privilege name in: " + authority);
        }
        ServiceAuthority.ServicePrivilege servicePrivilege = ServiceAuthority.ServicePrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        //return new ServiceAuthority(servicePrivilege);

        ServiceFilter filter = null;
        if (split.length > Constants.FILTER_POS) {
            String filterServiceId = split[Constants.FILTER_POS];
            String serviceId = authority.substring(authority.indexOf(split[Constants.FILTER_PARAMS_POS]));
            if (filterServiceId.equals(ServiceFilter.class.getSimpleName())) {
                filter = new ServiceAuthority.ServiceFilter(serviceId);
            } else {
                throw new IllegalArgumentException("Unknown authority representation.");
            }
        }

        return new ServiceAuthority(servicePrivilege, filter);

    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
/*    @Override
    public String getAuthority() {
        return servicePrivilege.getClass().getSimpleName() + Constants.DELIM + servicePrivilege.name();
    }*/

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return servicePrivilege.getClass().getSimpleName() + Constants.DELIM
                + servicePrivilege.name() + (filter == null ? "" : Constants.DELIM + filter);
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    @Override
    public Privilege getPrivilege() {
        return servicePrivilege;
    }

    public ServiceAuthority.ServiceFilter getFilter() {
        return this.filter;
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
        ServiceAuthority other = (ServiceAuthority) obj;
        if (getAuthority() == null) {
            if (other.getAuthority() != null)
                return false;
        } else if (!getAuthority().equals(other.getAuthority()))
            return false;
        return true;
    }


    public static class ServiceFilter implements SpaceAuthority.Filter<String> {

        private static final long serialVersionUID = 1L;

        private String serviceId;

        public ServiceFilter(String serviceId) {
            this.serviceId = serviceId;
        }

        @Override
        public String getExpression() {
            return serviceId;
        }

        @Override
        public boolean accept(String other) {
            if (serviceId.equals(other)) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "ServiceFilter " + serviceId ;
        }
    }

}
