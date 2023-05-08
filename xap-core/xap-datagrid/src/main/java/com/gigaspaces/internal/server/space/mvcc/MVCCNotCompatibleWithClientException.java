package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.version.PlatformLogicalVersion;

public class MVCCNotCompatibleWithClientException extends RuntimeException{

    private static final long serialVersionUID = -2828336091395733770L;


    public MVCCNotCompatibleWithClientException(PlatformLogicalVersion logicalVersion) {
        super("MVCC is only supported from client version 16.4.0 and above, client version in use: " + logicalVersion);
    }

}
