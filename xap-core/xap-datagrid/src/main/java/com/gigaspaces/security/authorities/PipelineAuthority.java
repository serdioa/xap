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
    private final PipelineFilter filter;

    public PipelineAuthority(PipelinePrivilege pipelinePrivilege) {
        this(pipelinePrivilege, null);
    }

    public PipelineAuthority(PipelinePrivilege pipelinePrivilege, PipelineFilter filter) {
        this.pipelinePrivilege = pipelinePrivilege;
        this.filter = filter;
    }

    public static PipelineAuthority valueOf(String authority) {
        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!PipelinePrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Authority name in: " + authority);
        }

        PipelinePrivilege pipelinePrivilege = PipelinePrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);

        PipelineFilter filter = null;
        if (split.length > Constants.FILTER_POS) {
            String filterPipelineId = split[Constants.FILTER_POS];
            String pipelineId = authority.substring(authority.indexOf(split[Constants.FILTER_PARAMS_POS]));
            if (filterPipelineId.equals(PipelineFilter.class.getSimpleName())) {
                filter = new PipelineAuthority.PipelineFilter(pipelineId);
            } else {
                throw new IllegalArgumentException("Unknown authority representation.");
            }
        }

        return new PipelineAuthority(pipelinePrivilege, filter);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return pipelinePrivilege.getClass().getSimpleName() + Constants.DELIM + pipelinePrivilege.name()
                + (filter == null ? "" : Constants.DELIM + filter);
    }

    public PipelineFilter getFilter() {
        return filter;
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

    public static class PipelineFilter implements SpaceAuthority.Filter<String> {

        private static final long serialVersionUID = 1L;

        private String pipelineId;

        public PipelineFilter(String pipelineId) {
            this.pipelineId = pipelineId;
        }

        @Override
        public String getExpression() {
            return pipelineId;
        }

        @Override
        public boolean accept(String other) {
            if (pipelineId.equals(other)) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "PipelineFilter " + pipelineId;
        }
    }

}
