package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;

public class TokenCredentialsProvider extends  CredentialsProvider {

    private TokenUserDetails tokenUserDetails;


    public TokenCredentialsProvider(final String token) {
        this.tokenUserDetails = new TokenUserDetails() {
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
        };
    }

    @Override
    public UserDetails getUserDetails() {
        return tokenUserDetails;
    }
}
