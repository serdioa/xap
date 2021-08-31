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

package org.openspaces.memcached;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.openspaces.core.GigaSpace;
import org.openspaces.memcached.protocol.MemcachedCommandHandler;
import org.openspaces.memcached.protocol.SessionStatus;
import org.openspaces.memcached.protocol.UnifiedProtocolDecoder;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryCommandDecoder;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryResponseEncoder;
import org.openspaces.memcached.protocol.text.MemcachedCommandDecoder;
import org.openspaces.memcached.protocol.text.MemcachedFrameDecoder;
import org.openspaces.memcached.protocol.text.MemcachedResponseEncoder;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @author kimchy (shay.banon)
 */
public class MemCacheDaemon implements InitializingBean, DisposableBean, BeanNameAware, ServiceDetailsProvider, ServiceMonitorsProvider {

    protected final Log logger = LogFactory.getLog(getClass());

    public final static String memcachedVersion = "0.9";

    private GigaSpace space;

    private String beanName = "memcached";

    private String protocol = "dual";

    private String host;

    private int port;

    private int portRetries = 20;

    private boolean threaded = true;

    private int frameSize = 32768 * 1024;
    private int idleTime;

    private int boundedPort;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private SpaceCache cache;

    public void setSpace(GigaSpace space) {
        this.space = space;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPortRetries(int portRetries) {
        this.portRetries = portRetries;
    }

    public void setBeanName(String name) {
        this.beanName = name;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setThreaded(boolean threaded) {
        this.threaded = threaded;
    }

    public void afterPropertiesSet() throws Exception {
        cache = new SpaceCache(space);

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        boolean verbose = false;
        MemcachedCommandHandler commandHandler = new MemcachedCommandHandler(cache, memcachedVersion, verbose, idleTime);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .channel(NioServerSocketChannel.class)
                .group(bossGroup, workerGroup)
                .option(ChannelOption.SO_SNDBUF, 65536)
                .option(ChannelOption.SO_RCVBUF, 65536)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if ("binary".equalsIgnoreCase(protocol)) {
                            ch.pipeline().addLast(
                                    new MemcachedBinaryCommandDecoder(),
                                    commandHandler,
                                    new MemcachedBinaryResponseEncoder());
                        } else if ("text".equalsIgnoreCase(protocol)) {
                            SessionStatus status = new SessionStatus().ready();
                            ch.pipeline().addLast(
                                    new MemcachedFrameDecoder(status, frameSize),
                                    new MemcachedCommandDecoder(status),
                                    commandHandler,
                                    new MemcachedResponseEncoder());
                        } else {
                            ch.pipeline().addLast(new UnifiedProtocolDecoder(commandHandler, threaded));
                        }
                    }
                });

        serverChannel = bind(bootstrap, host, port, portRetries);
        boundedPort = ((InetSocketAddress)serverChannel.localAddress()).getPort();
        logger.info("memcached started on port [" + boundedPort + "]");
    }

    private static Channel bind(ServerBootstrap bootstrap, String host, int port, int portRetries)
            throws Exception {
        final InetAddress address = host != null ? InetAddress.getByName(host) : null;

        Exception lastException = null;
        for (int i = 0; i < portRetries; i++) {
            try {
                return bootstrap.bind(new InetSocketAddress(address, port + i)).sync().channel();
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw lastException;
    }

    public void destroy() throws Exception {
        bossGroup.shutdownGracefully().sync();
        workerGroup.shutdownGracefully().sync();
        serverChannel.closeFuture().sync();
        try {
            cache.close();
        } catch (IOException e) {
            throw new RuntimeException("exception while closing storage", e);
        }
        logger.info("memcached destroyed");
    }

    public ServiceDetails[] getServicesDetails() {
        return new ServiceDetails[]{new MemcachedServiceDetails(beanName, space.getName(), boundedPort)};
    }

    public ServiceMonitors[] getServicesMonitors() {
        return new ServiceMonitors[]{new MemcachedServiceMonitors(beanName, cache.getGetCmds(), cache.getSetCmds(), cache.getGetHits(), cache.getGetMisses())};
    }
}
