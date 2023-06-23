package com.gigaspaces.security.authorities;

public class PipelineAuthority implements InternalAuthority {

    /**
     * Defines monitoring privileges
     */
    public enum PipelinePrivilege implements Privilege {
        /**
         * managing of roles
         */
        CREATE,
        /**
         * managing of pu/space settings
         */
        START,
        /**
         * managing of users
         */
        STOP,
        /**
         * managing of identity provider settings
         */
        DELETE;

        @Override
        public String toString() {
            switch (this) {
                case CREATE:
                    return "Create pipeline";
                case START:
                    return "Start pipeline";
                case STOP:
                    return "Stop pipeline";
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
