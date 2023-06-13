package org.openspaces.core.executor.mvcc.protocol;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.server.TransactionManager;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.mvcc.IMVCCTask;
import org.openspaces.core.executor.mvcc.exception.MVCCRetryTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.time.Duration;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractOnePhaseTask implements IMVCCTask<AbstractMVCCProtocolTaskResult> {
    private static final long serialVersionUID = 5714510450727020854L;
    private static final Logger logger = LoggerFactory.getLogger("org.openspaces.core.executor.mvcc.task.one-phase");
    public static final long DEFAULT_TRANSACTION_LEASE = Duration.ofMinutes(5).toMillis();
    @TaskGigaSpace
    private transient GigaSpace space;
    private final MVCCGenerationsState generationsState;

    protected AbstractOnePhaseTask(MVCCGenerationsState generationsState) {
        this.generationsState = generationsState;
    }

    private Transaction createEmbeddedTransaction(IDirectSpaceProxy directSpaceProxy) throws LeaseDeniedException, RemoteException {
        final SpaceImpl spaceImpl = directSpaceProxy.getSpaceImplIfEmbedded();
        final TransactionManager embeddedTransactionManager = spaceImpl.getContainer().getEmbeddedTransactionManager();
        return TransactionFactory.create(embeddedTransactionManager, DEFAULT_TRANSACTION_LEASE).transaction;
    }

    private IDirectSpaceProxy getDirectProxy() {
        IDirectSpaceProxy directSpaceProxy = space.getSpace().getDirectProxy();
        directSpaceProxy.setMVCCGenerationsState(generationsState);
        return directSpaceProxy;
    }

    @Override
    public AbstractMVCCProtocolTaskResult execute() throws Exception {
        final IDirectSpaceProxy directProxy = getDirectProxy();
        final Transaction transaction = createEmbeddedTransaction(directProxy);
        try {
            AbstractMVCCProtocolTaskResult result = executeTask(directProxy, transaction);
            result.addActiveGeneration(getActiveGeneration());
            transaction.commit();
            clearGenerationState();
            return result;
        } catch (MVCCRetryTaskException e) {
            logger.warn("Caught while executing prepare task", e);
            logger.info("Aborting transaction {} with generation {} and retry.",  transaction, getActiveGeneration());
            transaction.abort();
            return execute();
        } //TODO @sagiv should abort with any exception. and we should also take care of active generation set
    }

    @Override
    public long getActiveGeneration() {
        return generationsState.getNextGeneration();
    }

    private void clearGenerationState() {
        space.getSpace().getDirectProxy().setMVCCGenerationsState(null);
    }

    protected abstract AbstractMVCCProtocolTaskResult executeTask(IDirectSpaceProxy proxy, Transaction transaction)
            throws MVCCRetryTaskException, TransactionException, RemoteException, UnusableEntryException, InterruptedException;
}
