/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @(#)LookupManager.java 1.0   20/09/2000  20:00PM
 */
package com.j_spaces.core;

import com.j_spaces.kernel.log.JProperties;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lookup.ServiceIDListener;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_URL_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_URL_PROP;

/**
 * This class <code>LookupManager</code> provides : <ul> <li> Registration forever of JSpaces and
 * JSpace containers with all discovered Lookup Services using <code>registerForever()</code>
 * method. <li> Registering container and JSpaces with rmiregistry. <li> Registering JSpace and
 * JSpace containers with exists ServiceID. <li> LeaseRenewManager that renews each lease as necessary to achieve a desired
 * expiration time. <li> Dynamic registration of all JSpaces and JSpace container with all
 * discovered Lookup Services. <li> Notification of LookupEvents. <li> Clean up LookupManager. <li>
 * Logger messages. </ul>
 *
 * The <code>registerForever()</code> method registering forever(Lease.FOREVER) JSpace with with all
 * Lookup Services that discovered in the Network. The <code>cleanup()</code> method provides
 * unregistration of all spaces that was registered FOREVER from all Lookup Services. LookupManager
 * support container and JSpace registration with Rmi registry depends
 * "com.j_spaces.core.container.use_rmi_registry" property in <container-name>-config.xml file
 * [false] - Do not use rmi registry. [true]  - Use rmi registry and Lookup services.
 *
 * When space or container goes down(reboot) all information about container stored in
 * <%GSDir%>\config\<%hostname of container%>.xml file. The information is - Attributes of every
 * space in container. - ServiceID of every space. - All information about every
 * LookupService(ServiceID). - Which spaces registered on every LookupService. Using this
 * information after reboot of computer(LookupService) where spaces was registered, all spaces will
 * be registered with exists ServiceID.
 *
 * This class using <code>LeaseRenewManager</code> class to renews each lease of registered space as
 * necessary when a desired expiration time achieved.
 *
 * Every Lookup Service that was discovered in the Network, dynamically will registered with all
 * spaces that stored in this <code>LookupManager</code> object.
 *
 * Every LookupService that was discovered in the Network will get notification to
 * <code>discovered()</code> method. Whether one or more lookup service registrars has been
 * discarded this class will get notification to <code>discarded</code> method.
 *
 * <code>terminate</code> function unregistering container and JSpaces from all lookup services and
 * terminate LookupManager.//TODO
 *
 * Constructor of this class get as parameter <code>Log</code> object that provides writing all
 * Log-statements that existing in this class to Log(it can be File, Socket). If this
 * <code>Log</code> object equals to null, all Log-statements will be disable.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/
@com.gigaspaces.api.InternalApi
public class LookupManager implements ServiceIDListener, LeaseListener {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_LOOKUPMANAGER);

    private final String m_ContainerName;
    private final Hashtable<String, IJSpace> _registeredSpaces;
    private final Context m_jndiContext;

    public LookupManager(String containerName) throws NamingException, IOException {
        m_ContainerName = containerName;
        _registeredSpaces = new Hashtable<String, IJSpace>();

        // check if container will work versus rmiregistry
        boolean jndiFlag = Boolean.parseBoolean(JProperties.getContainerProperty(containerName, LOOKUP_JNDI_ENABLED_PROP,
                LOOKUP_JNDI_ENABLED_DEFAULT));

        m_jndiContext = jndiFlag ? createInternalContext(containerName) : null;
    }

    /**
     * Call back function that receive as parameter the <code>serviceID</code> of registered
     * service.
     *
     * @param serviceID ServiceID of registered service.
     **/
    public void serviceIDNotify(ServiceID serviceID) {
        if (_logger.isInfoEnabled()) {
            _logger.info("<" + m_ContainerName + "> container registered with " + serviceID.toString() + " serviceID successfully.");
        }
    }

    /**
     * Called by the LeaseRenewalManager when it cannot renew a lease that it is managing, and the
     * lease's desired expiration time has not yet been reached.
     *
     * @param leaseManagerEvent Instance of LeaseRenewalEvent containing information about the lease
     *                          that the <code>LeaseRenewalManager</code> was unable to renew.
     **/
    public void notify(LeaseRenewalEvent leaseManagerEvent) {
        if (_logger.isDebugEnabled()) {
            Throwable error = leaseManagerEvent.getException();
            _logger.debug("LeaseRenewalManager cannot renew this lease <" +
                    leaseManagerEvent.getLease().getExpiration() + ">" +
                    (error != null ? " - " + error.toString() : ""));
        }
    }

    /**
     * Parse LookupLocator URLs and build array of <code>LookupLocator</code>. List of LookupLocator
     * URLs should be separates by comma. For example: "host1:4180,host2:4180,host3:4180"
     *
     * @param lookupLocatorURLs List of LookupLocators URLs, separates by ",".
     * @return LookupLocator[] Array of initialized <code>LookupLocator</code>.
     **/
    public static LookupLocator[] buildLookupLocators(String lookupLocatorURLs) {
        String locatorURL = null;
        ArrayList<LookupLocator> locatorList = new ArrayList<LookupLocator>();
        if (lookupLocatorURLs != null && lookupLocatorURLs.length() > 0) {
            StringTokenizer st = new StringTokenizer(lookupLocatorURLs, ",");

            while (st.hasMoreTokens()) {
                try {
                    locatorURL = st.nextToken().trim();

                    // don't create empty lookup locator
                    if (locatorURL.isEmpty() || locatorURL.equals("\"\""))
                        continue;

                    if (!locatorURL.startsWith("jini://"))
                        locatorURL = "jini://" + locatorURL;

                    LookupLocator lookupLocator = new LookupLocator(locatorURL);
                    locatorList.add(lookupLocator);
                } catch (MalformedURLException ex) {
                    if (_logger.isWarnEnabled()) {
                        _logger.warn("Unicast discovery failed for LookupService: " + locatorURL + " - " + ex.toString(), ex);
                    }
                }
            }
        }

        return locatorList.toArray(new LookupLocator[locatorList.size()]);
    }

    /**
     * Registering forever proxy of JSpace with all Lookup services that exists in the Network.
     *
     * @param spaceProxy JSpace proxy.
     **/
    public void register(IJSpace spaceProxy, String containerName) {
        // Get name of this space
        String spaceName = spaceProxy.getName();

        // Deposit space-proxy to <code>m_jspaceDepot</code> HashTable
        _registeredSpaces.put(spaceName, spaceProxy);
    }

    /**
     * Unregistering appropriate Space from all Lookup Services that this space was registered.
     *
     * @param spaceName JSpace name.
     *
     *                  is not known to the grantor of the lease. This can occur when a lease
     *                  expires or has been canceled.
     **/
    public void unregister(String spaceName) {
        _registeredSpaces.remove(spaceName);
    }

    /**
     * Unregister all JSpace and JSpace container from all Lookup Services.
     **/
    public void terminate() throws RemoteException {
        Hashtable<String, IJSpace> tmpDepot = (Hashtable<String, IJSpace>) _registeredSpaces.clone();
        for (String spaceName : tmpDepot.keySet())
            unregister(spaceName);

        if (_logger.isDebugEnabled())
            _logger.debug("All spaces unregistered successfully");
    }

    private static Context createInternalContext(String containerName) throws NamingException {
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.rmi.registry.RegistryContextFactory");
        env.put(Context.PROVIDER_URL, "rmi://" + JProperties.getContainerProperty(containerName, LOOKUP_JNDI_URL_PROP, LOOKUP_JNDI_URL_DEFAULT));
        return new InitialContext(env);
    }
}
