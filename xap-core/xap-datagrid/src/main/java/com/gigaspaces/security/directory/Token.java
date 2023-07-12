package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;

public class Token implements TokenUserDetails {

    private static final long serialVersionUID = 1L;

    private String token;

    public Token(String token) {
        this.token = token;
    }

    @Override
    public String token() {
        return token;
    }

    @Override
    public Authority[] getAuthorities() {
        return new Authority[0];
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public String getUsername() {
        return null;
    }

    @Override
    public String toString() {
        return "Token{" +
                "token='" + token + '\'' +
                '}';
    }
}
