package com.gigaspaces.internal.server.space.tiered_storage.transaction;


import com.gigaspaces.internal.server.storage.IEntryHolder;

@com.gigaspaces.api.InternalApi
public class TieredStorageUpdateBulkOperationRequest extends TieredStorageBulkOperationRequest {
    public TieredStorageUpdateBulkOperationRequest(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    @Override
    public boolean isUpdateOperation() {
        return true;
    }
}
