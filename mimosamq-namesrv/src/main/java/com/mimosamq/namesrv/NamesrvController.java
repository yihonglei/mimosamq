package com.mimosamq.namesrv;

import com.mimosamq.common.constant.LoggerName;
import com.mimosamq.common.future.FutureTaskExt;
import com.mimosamq.common.logging.InternalLogger;
import com.mimosamq.common.logging.InternalLoggerFactory;
import com.mimosamq.namesrv.config.NamesrvConfig;
import com.mimosamq.common.thread.ThreadFactoryImpl;
import com.mimosamq.common.util.NetworkUtil;
import com.mimosamq.namesrv.processor.ClientRequestProcessor;
import com.mimosamq.namesrv.processor.DefaultRequestProcessor;
import com.mimosamq.namesrv.routeinfo.BrokerHousekeepingService;
import com.mimosamq.namesrv.routeinfo.RouteInfoManager;
import com.mimosamq.remoting.RemotingClient;
import com.mimosamq.remoting.RemotingServer;
import com.mimosamq.remoting.netty.*;
import com.mimosamq.remoting.protocol.RequestCode;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * @author yihonglei
 */
public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(LoggerName.NAMESRV_LOGGER_NAME);
    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    private RemotingServer remotingServer;
    private final RouteInfoManager routeInfoManager;

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    private final ScheduledExecutorService scanExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    private final BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService defaultExecutor;
    private ExecutorService clientRequestExecutor;

    private BlockingQueue<Runnable> defaultThreadPoolQueue;
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.routeInfoManager = new RouteInfoManager(namesrvConfig);
    }

    public boolean initialize() {
        initiateNetworkComponents();
        initiateThreadExecutors();
        registerProcessor();
        startScheduleService();
        return true;
    }

    private void initiateNetworkComponents() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
    }

    private void initiateThreadExecutors() {
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        this.defaultExecutor = new ThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<>(runnable, value);
            }
        };

        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        this.clientRequestExecutor = new ThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<>(runnable, value);
            }
        };
    }

    private void registerProcessor() {
        // Support get route info only temporarily
        ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

        this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
    }

    private void startScheduleService() {
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
                5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
    }

    public void start() {
        this.remotingServer.start();
        this.routeInfoManager.start();
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }
}
