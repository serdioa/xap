package org.openspaces.core.executor.mvcc.protocol;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCHandler;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.j_spaces.core.admin.ZKCollocatedClientConfig;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.log.JProperties;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionManager;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.mvcc.IMVCCTask;
import org.openspaces.core.executor.mvcc.exception.MVCCRetryTaskException;
import org.openspaces.core.executor.mvcc.exception.MVCCZooKeeperHandlerCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ATTRIBUET_STORE_HANDLER_CLASS_NAME;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractPrepareTwoPhaseTask implements IMVCCTask<AbstractMVCCProtocolTaskResult> {
    private static final long serialVersionUID = 2137247152783484363L;
    private static final Logger logger =
            LoggerFactory.getLogger("org.openspaces.core.executor.mvcc.task.prepare-two-phase");
    public static final long DEFAULT_TRANSACTION_LEASE = Duration.ofMinutes(5).toMillis();
    @TaskGigaSpace
    private transient GigaSpace space;
    private MVCCGenerationsState generationsState;
    private ZKCollocatedClientConfig zkCollocatedClientConfig;
    private ZooKeeperMVCCHandler handler;
    private boolean isFirstTry = true;

    private final Set<Long> activeGenerations = new HashSet<>();

    protected AbstractPrepareTwoPhaseTask(MVCCGenerationsState generationsState) {
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
        if (!isFirstTry) {
            this.generationsState = getNextMVCCGenerationsState();
        }
        final IDirectSpaceProxy directProxy = getDirectProxy();
        final Transaction transaction = createEmbeddedTransaction(directProxy);
        try {
            AbstractMVCCProtocolTaskResult result = executePrepareTask(directProxy, transaction);
            result.setEmbeddedTransactionId(((ServerTransaction) transaction).id);
            result.addActiveGeneration(getActiveGeneration());
            this.activeGenerations.forEach(result::addActiveGeneration);
            return result;
        } catch (MVCCRetryTaskException e) {
            isFirstTry = false;
            logger.warn("Caught while executing prepare task", e);
            logger.info("Aborting transaction {} with generation {} and retry.",  transaction, getActiveGeneration());
            transaction.abort();
            this.activeGenerations.add(getActiveGeneration());
            return execute();
        }  //TODO @sagiv should abort with any exception. and we should also take care of active generation set
    }

    private  MVCCGenerationsState getNextMVCCGenerationsState() {
        if (zkCollocatedClientConfig == null) {
            zkCollocatedClientConfig = getZKCollocatedClientConfig();
        }
        if (handler == null) {
            handler = new ZooKeeperMVCCHandler(createZooKeeperAttributeStore(), space.getSpaceName());
        }
        return handler.getNextGenerationsState();
    }

    private ZKCollocatedClientConfig getZKCollocatedClientConfig() {
        final String spaceName = space.getSpace().getName();
        final String containerName = space.getSpace().getContainerName();
        final String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
        Properties props = JProperties.getSpaceProperties(fullSpaceName);
        return new ZKCollocatedClientConfig(props);
    }

    private AttributeStore createZooKeeperAttributeStore() {
        try {
            Constructor constructor =
                    ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                            .getConstructor(ZKCollocatedClientConfig.class);
            return (AttributeStore) constructor.newInstance(zkCollocatedClientConfig);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to create attribute store ",e);
            }
            throw new MVCCZooKeeperHandlerCreationException(
                    "Failed to create attribute store for zookeeper MVCC handler.", e);
        }
    }

    @Override
    public long getActiveGeneration() {
        return generationsState.getNextGeneration();
    }

    protected abstract AbstractMVCCProtocolTaskResult executePrepareTask(IDirectSpaceProxy proxy, Transaction transaction)
            throws MVCCRetryTaskException, TransactionException, RemoteException;
}
