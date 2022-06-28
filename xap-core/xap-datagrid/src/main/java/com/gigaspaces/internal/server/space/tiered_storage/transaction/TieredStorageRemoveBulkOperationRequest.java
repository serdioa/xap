package com.gigaspaces.internal.server.space.tiered_storage.transaction;


import com.gigaspaces.internal.server.storage.IEntryHolder;

@com.gigaspaces.api.InternalApi
public class TieredStorageRemoveBulkOperationRequest extends TieredStorageBulkOperationRequest {

    public TieredStorageRemoveBulkOperationRequest(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    @Override
    public boolean isRemoveOperation() {
        return true;
    }
}
