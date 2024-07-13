package com.mimosamq.broker;

import com.mimosamq.broker.client.ClientHousekeepingService;
import com.mimosamq.broker.filter.FilterServerManager;
import com.mimosamq.broker.help.BrokerPathConfigHelper;
import com.mimosamq.broker.out.BrokerOuterAPI;
import com.mimosamq.broker.processor.SendMessageProcessor;
import com.mimosamq.broker.topic.TopicConfigManager;
import com.mimosamq.broker.topic.TopicQueueMappingManager;
import com.mimosamq.broker.config.BrokerConfig;
import com.mimosamq.common.constant.LoggerName;
import com.mimosamq.common.constant.PermName;
import com.mimosamq.common.logging.InternalLogger;
import com.mimosamq.common.logging.InternalLoggerFactory;
import com.mimosamq.common.thread.ThreadFactoryImpl;
import com.mimosamq.remoting.RemotingServer;
import com.mimosamq.remoting.netty.NettyClientConfig;
import com.mimosamq.remoting.netty.NettyRemotingServer;
import com.mimosamq.remoting.netty.NettyServerConfig;
import com.mimosamq.remoting.protocol.RequestCode;
import com.mimosamq.remoting.protocol.TopicConfig;
import com.mimosamq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import com.mimosamq.remoting.protocol.body.TopicConfigSerializeWrapper;
import com.mimosamq.remoting.protocol.namesrv.RegisterBrokerResult;
import com.mimosamq.remoting.protocol.statictopic.TopicQueueMappingDetail;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author yihonglei
 */
public class BrokerController {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(LoggerName.BROKER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected RemotingServer remotingServer;

    protected BrokerOuterAPI brokerOuterAPI;
    protected TopicConfigManager topicConfigManager;
    protected TopicQueueMappingManager topicQueueMappingManager;
    protected ScheduledExecutorService scheduledExecutorService;
    protected final ClientHousekeepingService clientHousekeepingService;
    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    protected final SendMessageProcessor sendMessageProcessor;
    protected ExecutorService sendMessageExecutor;

    protected final BlockingQueue<Runnable> sendThreadPoolQueue;

    protected final FilterServerManager filterServerManager;

    protected volatile boolean shutdown = false;

    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.topicQueueMappingManager = new TopicQueueMappingManager(this);

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        }
        this.filterServerManager = new FilterServerManager(this);
        this.sendThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = BrokerPathConfigHelper.getBrokerConfigPath();
        }
    }


    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    private void updateNamesrvAddr() {
        this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
    }

    public boolean initialize() throws CloneNotSupportedException {
        boolean result = this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();

        if (result) {
            initializeRemotingServer();
            initializeResources();
            registerProcessor();
            initializeScheduledTasks();
        }

        return result;
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
    }

    /**
     * Initialize resources including remoting server and thread executors.
     */
    protected void initializeResources() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryImpl("BrokerControllerScheduledThread"));

        this.sendMessageExecutor = new ThreadPoolExecutor(
                this.brokerConfig.getSendMessageThreadPoolNums(),
                this.brokerConfig.getSendMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.sendThreadPoolQueue,
                new ThreadFactoryImpl("SendMessageThread_"));

    }

    public void registerProcessor() {
        /*
         * SendMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
    }

    protected void initializeScheduledTasks() {
        this.updateNamesrvAddr();
        log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
        // also auto update namesrv if specify
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.updateNamesrvAddr();
            } catch (Throwable e) {
                log.error("Failed to update nameServer address list", e);
            }
        }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
    }

    public void start() throws Exception {
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
            } catch (Throwable e) {
                BrokerController.log.error("registerBrokerAll Exception", e);
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

    }

    public void shutdown() {
        shutdown = true;

        this.unregisterBrokerAll();

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        shutdownScheduledExecutorService(this.scheduledExecutorService);

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }
    }

    protected void shutdownScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService == null) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            BrokerController.log.warn("shutdown ScheduledExecutorService was Interrupted!  ", ignore);
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {

        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();

        topicConfigWrapper.setDataVersion(this.getTopicConfigManager().getDataVersion());
        topicConfigWrapper.setTopicConfigTable(this.getTopicConfigManager().getTopicConfigTable());

        topicConfigWrapper.setTopicQueueMappingInfoMap(this.getTopicQueueMappingManager().getTopicQueueMappingTable().entrySet().stream().map(
                entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp = new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    protected void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
                                       TopicConfigSerializeWrapper topicConfigWrapper) {

        if (shutdown) {
            BrokerController.log.info("BrokerController#doRegisterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                topicConfigWrapper,
                this.filterServerManager.buildNewFilterServerList(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMills(),
                this.brokerConfig.isEnableSlaveActingMaster(),
                this.brokerConfig.isCompressedRegister(),
                this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null);

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    protected void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList, boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult != null) {
                if (checkOrderConfig) {
                    // this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
                break;
            }
        }
    }

    private boolean needRegister(final String clusterName,
                                 final String brokerAddr,
                                 final String brokerName,
                                 final long brokerId,
                                 final int timeoutMills) {

        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    protected void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId());
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

}
