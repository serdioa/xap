package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.data_integration.consumer.CDCInfo;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSMessageExecutor extends SpaceActionExecutor {

    private final static Logger log = LoggerFactory.getLogger(GSMessageTask.class);

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GSMessageRequestInfo requestInfo = ((GSMessageRequestInfo) spaceRequestInfo);
        Transaction tx = requestInfo.getTx();
        SpaceDocument entry = null;
        try {
            CDCInfo cdcInfo = requestInfo.getCdcInfo();
            GSMessageTask.Mode mode = requestInfo.getMode();
            entry = requestInfo.getDocument();
            CDCInfo lastMsg = ((CDCInfo) space.getSpaceProxy().read(new CDCInfo().setStreamName(cdcInfo.getStreamName()), tx, 0));
            Long lastMsgID = lastMsg != null ? lastMsg.getMessageID() : 0;
            if (lastMsg != null) {
                if (cdcInfo.getMessageID() < lastMsgID) {
                    log.warn(String.format("Operation already occurred, ignoring message id: %s, last message id: %s", cdcInfo.getMessageID(), lastMsgID));
                    return null;
                }
            }
            space.getSpaceProxy().write(cdcInfo, tx, Lease.FOREVER, 0, WriteModifiers.UPDATE_OR_WRITE.getCode());
            switch (mode) {
                case INSERT:
                    try {
                        space.getSpaceProxy().write(entry, tx, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                    } catch (EntryAlreadyInSpaceException e) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            log.warn("Couldn't write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                            throw e; //might be the first time writing this to space
                        }
                        log.warn("Couldn't write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                    }
                    break;
                case UPDATE:
                    space.getSpaceProxy().write(entry, tx, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                    break;
                case DELETE:
                    if (space.getSpaceProxy().take(entry, tx, 0) == null) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            log.warn("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                            throw new RuntimeException("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                        }
                        log.warn("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                    }
                    break;
            }
        } catch (Exception e) {
            log.warn(String.format("Couldn't complete task execution for Object: %s", entry != null ? entry.getTypeName() : null));
            throw new RuntimeException(e);
        }
        return null;
    }

}
