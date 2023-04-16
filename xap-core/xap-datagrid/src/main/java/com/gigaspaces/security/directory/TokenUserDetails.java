package com.gigaspaces.security.directory;

public interface TokenUserDetails extends UserDetails {

    String token();

}
