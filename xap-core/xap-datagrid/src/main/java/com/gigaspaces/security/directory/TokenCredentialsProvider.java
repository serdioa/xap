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
