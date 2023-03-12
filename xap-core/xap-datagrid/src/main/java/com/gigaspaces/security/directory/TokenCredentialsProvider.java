package com.gigaspaces.security.directory;

import com.gigaspaces.security.Authority;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TokenCredentialsProvider extends  CredentialsProvider {

    private static final long serialVersionUID = 1L;

    private TokenUserDetails tokenUserDetails;

    public TokenCredentialsProvider(){
    }

    public TokenCredentialsProvider(final String token) {
        this.tokenUserDetails = new Token(token);
    }

    @Override
    public UserDetails getUserDetails() {
        return tokenUserDetails;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        out.writeObject(tokenUserDetails);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.tokenUserDetails = (TokenUserDetails) in.readObject();
    }

}
