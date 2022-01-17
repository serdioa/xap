package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceClass;

@SpaceClass(broadcast = true)
@com.gigaspaces.api.InternalApi
public class PojoBroadcastExtendNonBroadcastInvalid extends PojoNonBroadcast {
}
