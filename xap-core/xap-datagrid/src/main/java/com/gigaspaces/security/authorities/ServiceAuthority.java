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

    public ServiceAuthority(ServiceAuthority.ServicePrivilege servicePrivilege) {
        this.servicePrivilege = servicePrivilege;
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
        return new ServiceAuthority(servicePrivilege);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    @Override
    public String getAuthority() {
        return servicePrivilege.getClass().getSimpleName() + Constants.DELIM + servicePrivilege.name();
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

}
