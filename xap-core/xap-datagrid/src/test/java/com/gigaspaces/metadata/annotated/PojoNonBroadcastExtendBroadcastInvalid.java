package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceClass;

@SpaceClass(broadcast = false)
@com.gigaspaces.api.InternalApi
public class PojoNonBroadcastExtendBroadcastInvalid extends PojoBroadcast {
}
