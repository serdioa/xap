package com.j_spaces.kernel.locks;

/**
 * The basic interface for a Blob-Store lock object.
 * include method to get the ExternalLockObject that used in {@link BlobStoreLockManager}.
 * @author Sagiv Michael
 * @since 16.3.0
 */
public interface IBlobStoreLockObject extends ILockObject {

    /**
     * in case the locking object can be derived from the subject WHEN its not the subject itself
     *
     * @return the lock object
     */
    ILockObject getExternalLockObject();
}
