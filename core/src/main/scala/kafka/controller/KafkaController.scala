/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk._
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, LeaderAndIsrResponse, StopReplicaResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{Code, NodeExistsException}

import scala.collection._
import scala.util.Try
//集群是不会直接与zk进行交互的，都是通过Controller去获取元数据
object KafkaController extends Logging {
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  /**
   * ControllerEventThread will shutdown once it sees this event
   */
  private[controller] case object ShutdownEventThread extends ControllerEvent {
    def state = ControllerState.ControllerShutdown
    override def process(): Unit = ()
  }

}
//kafkaj集群控制器组件，管理与保存元数据
class KafkaController(val config: KafkaConfig, //kafkap配置文件
                      zkClient: KafkaZkClient,  //zk
                      time: Time,
                      metrics: Metrics, //实现指标监控工具类
                      initialBrokerInfo: BrokerInfo,//broker节点信息，主机名，端口号，所用监听器等
                      tokenManager: DelegationTokenManager, //实现Delegation token管理的工具类。Delegation token是一种轻量级的认证机制
                      threadNamePrefix: Option[String] = None //ccontroller端s事件处理线程名字前缀
                     ) extends Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  @volatile private var brokerInfo = initialBrokerInfo

  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  //集群元数据管理类，保存集群所有元数据
  val controllerContext = new ControllerContext

  //线程调度器，当前唯一负责定期执行Leader选举
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  //controller事件管理器，负责管理事件处理线程
  private[controller] val eventManager = new ControllerEventManager(config.brokerId,
    controllerContext.stats.rateAndTimeMetrics, _ => updateMetrics())
  //主题删除管理器，负责删除主题和日志
  val topicDeletionManager = new TopicDeletionManager(this, eventManager, zkClient)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this, stateChangeLogger)
  //副本状态机，负责副本状态转换
  val replicaStateMachine = new ReplicaStateMachine(config, stateChangeLogger, controllerContext, topicDeletionManager, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))
  //分区状态机，负责分区状态转换
  val partitionStateMachine = new PartitionStateMachine(config, stateChangeLogger, controllerContext, topicDeletionManager, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))
  //Controller节点ZooKeeper监听器
  private val controllerChangeHandler = new ControllerChangeHandler(this, eventManager)
  //broker数量zookeeper监听器,每个broker启动时会在zk /broker/ids/创建一个临时节点
  private val brokerChangeHandler = new BrokerChangeHandler(this, eventManager)
  //brokers信息变更zookeeperj监听器集合
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty
  //主题数量zzookeeper监听器
  private val topicChangeHandler = new TopicChangeHandler(this, eventManager)
  //主题删除zookeeper监听器
  private val topicDeletionHandler = new TopicDeletionHandler(this, eventManager)
  //主题分区变更zookeeper监听器
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty
  //主题分区重分配zookeeper监听器
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(this, eventManager)
  //Preferred  leader选举zookeeperj监听器
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(this, eventManager)
  //ISR副本集合变更zookeeper监听器
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(this, eventManager)
  //日志路径变更zookeeper监听器
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(this, eventManager)

  //统计指标
  @volatile private var activeControllerId = -1  //当前controller所在brokerId
  @volatile private var offlinePartitionCount = 0   //统计集群中所有离线和处于不可用状态的主题分区数量
  @volatile private var preferredReplicaImbalanceCount = 0  //满足Preferred Leader选举条件的总分区数
  @volatile private var globalTopicCount = 0  //总主题数
  @volatile private var globalPartitionCount = 0  //总主题分区数

  /* single-thread scheduler to clean expired tokens */
  private val tokenCleanScheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "delegation-token-cleaner")

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value: Int = offlinePartitionCount
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value: Int = preferredReplicaImbalanceCount
    }
  )

  newGauge(
    "ControllerState",
    new Gauge[Byte] {
      def value: Byte = state.value
    }
  )

  newGauge(
    "GlobalTopicCount",
    new Gauge[Int] {
      def value: Int = globalTopicCount
    }
  )

  newGauge(
    "GlobalPartitionCount",
    new Gauge[Int] {
      def value: Int = globalPartitionCount
    }
  )

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  def epoch: Int = controllerContext.epoch

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
    //集群首次启动时，会将startup的事件Startup写入到事件队列中，然后启动对应的事件处理线程和ControllerChangeHandler zookeeper监听器
  def startup() = {
      //注册zk状态变更监听器，它是用于监听zk和broker的会话过期的
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      override def afterInitializingSession(): Unit = {
        eventManager.put(RegisterBrokerAndReelect)
      }
      override def beforeInitializingSession(): Unit = {
        val expireEvent = new Expire
        eventManager.clearAndPut(expireEvent)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        expireEvent.waitUntilProcessingStarted()
      }
    })
      //写入startup事件到事件队列
    eventManager.put(Startup)
      //启动ControllerEventThread线程，开始处理事件队列中的ControllerEvent
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    eventManager.close()
    onControllerResignation()
  }

  /**
   * On controlled shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  def controlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  private[kafka] def updateBrokerInfo(newBrokerInfo: BrokerInfo): Unit = {
    this.brokerInfo = newBrokerInfo
    zkClient.updateBrokerInfoInZk(newBrokerInfo)
  }

  private[kafka] def enableDefaultUncleanLeaderElection(): Unit = {
    eventManager.put(UncleanLeaderElectionEnable)
  }

  private def state: ControllerState = eventManager.state

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Registers controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
    //当选controller的后续逻辑
  private def onControllerFailover() {
    info("Reading controller epoch from ZooKeeper")
    readControllerEpochFromZooKeeper()
    info("Incrementing controller epoch in ZooKeeper")
    incrementControllerEpoch()
    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
      //注册各类zookeeper监听器
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    info("Deleting log dir event notifications")
      //删除日志路径变更和ISR变更通知
    zkClient.deleteLogDirEventNotifications()
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications()
    info("Initializing controller context")
      //初始化集群元数据
    initializeControllerContext()
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    info("Sending update metadata request")
      //给broker发送元数据更新请求
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    //启动副本状态机和分区状态机
    replicaStateMachine.startup()
    partitionStateMachine.startup()

    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment(controllerContext.partitionsBeingReassigned.keySet)
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = tokenManager.expireTokens,
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   */
    //卸任逻辑
  private def onControllerResignation() {
    debug("Resigning")
    // 取消zk监听器的注册
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // reset topic deletion manager
    topicDeletionManager.reset()

    // 关闭kafka线程调度器，取消定期的leader重选举
    kafkaScheduler.shutdown()
    //将统计字段全部清0
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0

    // 关闭token过期检查调度器
    if (tokenCleanScheduler.isStarted)
      tokenCleanScheduler.shutdown()

    //取消分区重分配监听器的注册
    unregisterPartitionReassignmentIsrChangeHandlers()
    // 关闭分区状态机
    partitionStateMachine.shutdown()
    //取消主题变更监听器的注册
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)
    //取消分区变更监听器注册
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)
    //取消主题删除监听器的注册
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)
    // shutdown replica state machine
    replicaStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)
    //清空集群元数据
    controllerContext.resetContext()

    info("Resigned")
  }

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas
   */
  private def onBrokerLogDirFailure(brokerIds: Seq[Int]) {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers. If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
    //处理broker启动的方法，也就是controllerd端对应集群新增Broker启动的方法
  private def onBrokerStartup(newBrokers: Seq[Int]) {
    info(s"New broker startup callback for ${newBrokers.mkString(",")}")
      //移除元数据中新增broker对应的副本集合,如果新broker之前有，就需要清理掉之前的元数据等信息。
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster
      //给集群现有Broker发送元数据更新请求，令它们感知到新增broker的到来
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    //将新增Broker上的所有副本设置为online状态，即可用状态
      replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains)
    }
    partitionsWithReplicasOnNewBrokers.foreach { case (tp, context) => onPartitionReassignment(tp, context) }
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
      //重启之前暂停的主题删除操作
      if (replicasForTopicsToBeDeleted.nonEmpty) {
      info(s"Some replicas ${replicasForTopicsToBeDeleted.mkString(",")} for topics scheduled for deletion " +
        s"${topicDeletionManager.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
      //为新增bbroker注册BrokerModificationsHandler监听器，允许controller监听他们在zk节点上d的数据变更
    registerBrokerModificationsHandler(newBrokers)
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(this, eventManager, brokerId)
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerModificationsHandler)
      brokerModificationsHandlers.put(brokerId, brokerModificationsHandler)
    }
  }

  private def unregisterBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Unregister BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      brokerModificationsHandlers.remove(brokerId).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   */
  //处理broker终止逻辑的方法
  private def onBrokerFailure(deadBrokers: Seq[Int]) {
    info(s"Broker failure callback for ${deadBrokers.mkString(",")}")
    //更新Controler元数据信息，将待删除的broker从元数据的replicasOnOfflineDirs移除
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    //将待移除的broker从元数据对象中处于已关闭状态的broker列表中去除
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info(s"Removed $deadBrokersThatWereShuttingDown from list of shutting down brokers.")
    //执行副本清扫工作，找出待移除Broker上的所有副本对象，执行相应操作。
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    //处理broker上的副本对象
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)
    //注销注册的BrokerModificationHandler监听器
    unregisterBrokerModificationsHandler(deadBrokers)
  }
  //向集群所有Broker发送更新元数据信息请求，把变更信息广播出去
  private def onBrokerUpdate(updatedBrokerId: Int) {
    info(s"Broker info update callback for $updatedBrokerId")
    //给集群所有broker发送updateMetadataRequest 让它们更新元数据
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Marks the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point. This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      !controllerContext.isReplicaOnline(partitionAndLeader._2.leaderAndIsr.leader, partitionAndLeader._1) &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader.toSeq, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline replica
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  private def onNewPartitionCreation(newPartitions: Set[TopicPartition]) {
    info(s"New partition creation callback for ${newPartitions.mkString(",")}")
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    if (!areReplicasInIsr(topicPartition, reassignedReplicas)) {
      info(s"New replicas ${reassignedReplicas.mkString(",")} for partition $topicPartition being reassigned not yet " +
        "caught up with the leader")
      val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicPartition).toSet
      val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicPartition)).toSet
      //1. Update AR in ZK with OAR + RAR.
      updateAssignedReplicasForPartition(topicPartition, newAndOldReplicas.toSeq)
      //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
      updateLeaderEpochAndSendRequest(topicPartition, controllerContext.partitionReplicaAssignment(topicPartition),
        newAndOldReplicas.toSeq)
      //3. replicas in RAR - OAR -> NewReplica
      startNewReplicasForReassignedPartition(topicPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
      info(s"Waiting for new replicas ${reassignedReplicas.mkString(",")} for partition ${topicPartition} being " +
        "reassigned to catch up with the leader")
    } else {
      //4. Wait until all replicas in RAR are in sync with the leader.
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicPartition).toSet -- reassignedReplicas.toSet
      //5. replicas in RAR -> OnlineReplica
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Seq(new PartitionAndReplica(topicPartition, replica)), OnlineReplica)
      }
      //6. Set AR to RAR in memory.
      //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
      //   a new AR (using RAR) and same isr to every broker in RAR
      moveReassignedPartitionLeaderIfRequired(topicPartition, reassignedPartitionContext)
      //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
      //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
      stopOldReplicasOfReassignedPartition(topicPartition, reassignedPartitionContext, oldReplicas)
      //10. Update AR in ZK with RAR.
      updateAssignedReplicasForPartition(topicPartition, reassignedReplicas)
      //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
      removePartitionsFromReassignedPartitions(Set(topicPartition))
      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * Trigger partition reassignment for the provided partitions if the assigned replicas are not the same as the
   * reassigned replicas (as defined in `ControllerContext.partitionsBeingReassigned`) and if the topic has not been
   * deleted.
   *
   * `partitionsBeingReassigned` must be populated with all partitions being reassigned before this method is invoked
   * as explained in the method documentation of `removePartitionFromReassignedPartitions` (which is invoked by this
   * method).
   *
   * @throws IllegalStateException if a partition is not in `partitionsBeingReassigned`
   */
  private def maybeTriggerPartitionReassignment(topicPartitions: Set[TopicPartition]) {
    val partitionsToBeRemovedFromReassignment = scala.collection.mutable.Set.empty[TopicPartition]
    topicPartitions.foreach { tp =>
      if (topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)) {
        error(s"Skipping reassignment of $tp since the topic is currently being deleted")
        partitionsToBeRemovedFromReassignment.add(tp)
      } else {
        val reassignedPartitionContext = controllerContext.partitionsBeingReassigned.get(tp).getOrElse {
          throw new IllegalStateException(s"Initiating reassign replicas for partition $tp not present in " +
            s"partitionsBeingReassigned: ${controllerContext.partitionsBeingReassigned.mkString(", ")}")
        }
        val newReplicas = reassignedPartitionContext.newReplicas
        val topic = tp.topic
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          if (assignedReplicas == newReplicas) {
            info(s"Partition $tp to be reassigned is already assigned to replicas " +
              s"${newReplicas.mkString(",")}. Ignoring request for partition reassignment.")
            partitionsToBeRemovedFromReassignment.add(tp)
          } else {
            try {
              info(s"Handling reassignment of partition $tp to new replicas ${newReplicas.mkString(",")}")
              // first register ISR change listener
              reassignedPartitionContext.registerReassignIsrChangeHandler(zkClient)
              // mark topic ineligible for deletion for the partitions being reassigned
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
              onPartitionReassignment(tp, reassignedPartitionContext)
            } catch {
              case e: Throwable =>
                error(s"Error completing reassignment of partition $tp", e)
                // remove the partition from the admin path to unblock the admin client
                partitionsToBeRemovedFromReassignment.add(tp)
            }
          }
        } else {
            error(s"Ignoring request to reassign partition $tp that doesn't exist.")
            partitionsToBeRemovedFromReassignment.add(tp)
        }
      }
    }
    removePartitionsFromReassignedPartitions(partitionsToBeRemovedFromReassignment)
  }

  private def onPreferredReplicaElection(partitions: Set[TopicPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info(s"Starting preferred replica leader election for partitions ${partitions.mkString(",")}")
    try {
      partitionStateMachine.handleStateChanges(partitions.toSeq, OnlinePartition, Option(PreferredReplicaPartitionLeaderElectionStrategy))
    } catch {
      case e: Throwable => error(s"Error completing preferred replica leader election for partitions ${partitions.mkString(",")}", e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
    }
  }

  private def incrementControllerEpoch(): Unit = {
    val newControllerEpoch = controllerContext.epoch + 1
    val setDataResponse = zkClient.setControllerEpochRaw(newControllerEpoch, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        controllerContext.epochZkVersion = setDataResponse.stat.getVersion
        controllerContext.epoch = newControllerEpoch
      case Code.NONODE =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        val createResponse = zkClient.createControllerEpochRaw(KafkaController.InitialControllerEpoch)
        createResponse.resultCode match {
          case Code.OK =>
            controllerContext.epoch = KafkaController.InitialControllerEpoch
            controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
          case Code.NODEEXISTS =>
            throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
          case _ =>
            val exception = createResponse.resultException.get
            error("Error while incrementing controller epoch", exception)
            throw exception
        }
      case _ =>
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
    }
    info(s"Epoch incremented to ${controllerContext.epoch}")
  }

  private def initializeControllerContext() {
    // update controller cache with delete topic information
    controllerContext.liveBrokers = zkClient.getAllBrokersInCluster.toSet
    controllerContext.allTopics = zkClient.getAllTopicsInCluster.toSet
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    zkClient.getReplicaAssignmentForTopics(controllerContext.allTopics.toSet).foreach {
      case (topicPartition, assignedReplicas) => controllerContext.updatePartitionReplicaAssignment(topicPartition, assignedReplicas)
    }
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // register broker modifications handlers
    registerBrokerModificationsHandler(controllerContext.liveBrokers.map(_.id))
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    initializePartitionReassignment()
    info(s"Currently active brokers in the cluster: ${controllerContext.liveBrokerIds}")
    info(s"Currently shutting brokers in the cluster: ${controllerContext.shuttingDownBrokerIds}")
    info(s"Current list of topics in the cluster: ${controllerContext.allTopics}")
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val topicDeleted = replicas.isEmpty
      val successful =
        if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicas.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info(s"Partitions undergoing preferred replica election: ${partitionsUndergoingPreferredReplicaElection.mkString(",")}")
    info(s"Partitions that completed preferred replica election: ${partitionsThatCompletedPreferredReplicaElection.mkString(",")}")
    info(s"Skipping preferred replica election for partitions due to topic deletion: ${pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")}")
    info(s"Resuming preferred replica election for partitions: ${pendingPreferredReplicaElections.mkString(",")}")
    pendingPreferredReplicaElections
  }

  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = zkClient.getPartitionReassignment
    info(s"Partitions being reassigned: $partitionsBeingReassigned")

    controllerContext.partitionsBeingReassigned ++= partitionsBeingReassigned.iterator.map { case (tp, newReplicas) =>
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(this, eventManager, tp)
      tp -> new ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler)
    }
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    val topicsWithOfflineReplicas = controllerContext.allTopics.filter { topic => {
      val replicasForTopic = controllerContext.replicasForTopic(topic)
      replicasForTopic.exists(r => !controllerContext.isReplicaOnline(r.replica, r.topicPartition))
    }}
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info(s"List of topics to be deleted: ${topicsToBeDeleted.mkString(",")}")
    info(s"List of topics ineligible for deletion: ${topicsIneligibleForDeletion.mkString(",")}")
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
      stateChangeLogger, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq) {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    }
  }

  private def areReplicasInIsr(partition: TopicPartition, replicas: Seq[Int]): Boolean = {
    zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
      replicas.forall(leaderIsrAndControllerEpoch.leaderAndIsr.isr.contains)
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicPartition)
    controllerContext.updatePartitionReplicaAssignment(topicPartition, reassignedReplicas)
    if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Option(ReassignPartitionLeaderElectionStrategy))
    } else {
      // check if the leader is alive or not
      if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicPartition, oldAndNewReplicas, reassignedReplicas)
      } else {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
        partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Option(ReassignPartitionLeaderElectionStrategy))
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, NonExistentReplica)
  }

  private def updateAssignedReplicasForPartition(partition: TopicPartition,
                                                 replicas: Seq[Int]) {
    controllerContext.updatePartitionReplicaAssignment(partition, replicas)
    val setDataResponse = zkClient.setTopicAssignmentRaw(partition.topic, controllerContext.partitionReplicaAssignmentForTopic(partition.topic))
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Updated assigned replicas for partition $partition being reassigned to ${replicas.mkString(",")}")
        // update the assigned replica list after a successful zookeeper write
        controllerContext.updatePartitionReplicaAssignment(partition, replicas)
      case Code.NONODE => throw new IllegalStateException(s"Topic ${partition.topic} doesn't exist")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(new PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(partition: TopicPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, partition,
            updatedLeaderIsrAndControllerEpoch, newAssignedReplicas, isNew = false)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.trace(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with new assigned replica " +
          s"list ${newAssignedReplicas.mkString(",")} to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $partition")
      case None => // fail the reassignment
        stateChangeLog.error("Failed to send LeaderAndIsr request with new assigned replica list " +
          s"${newAssignedReplicas.mkString( ",")} to leader for partition being reassigned $partition")
    }
  }

  private def registerPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      val partitionModificationsHandler = new PartitionModificationsHandler(this, eventManager, topic)
      partitionModificationsHandlers.put(topic, partitionModificationsHandler)
    }
    partitionModificationsHandlers.values.foreach(zkClient.registerZNodeChangeHandler)
  }

  private[controller] def unregisterPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers() {
    controllerContext.partitionsBeingReassigned.values.foreach(_.unregisterReassignIsrChangeHandler(zkClient))
  }

  private def readControllerEpochFromZooKeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    val epochAndStatOpt = zkClient.getControllerEpoch
    epochAndStatOpt.foreach { case (epoch, stat) =>
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = stat.getVersion
      info(s"Initialized controller epoch to ${controllerContext.epoch} and zk version ${controllerContext.epochZkVersion}")
    }
  }

  /**
   * Remove partition from partitions being reassigned in ZooKeeper and ControllerContext. If the partition reassignment
   * is complete (i.e. there is no other partition with a reassignment in progress), the reassign_partitions znode
   * is deleted.
   *
   * `ControllerContext.partitionsBeingReassigned` must be populated with all partitions being reassigned before this
   * method is invoked to avoid premature deletion of the `reassign_partitions` znode.
   */
  private def removePartitionsFromReassignedPartitions(partitionsToBeRemoved: Set[TopicPartition]) {
    partitionsToBeRemoved.map(controllerContext.partitionsBeingReassigned).foreach { reassignContext =>
      reassignContext.unregisterReassignIsrChangeHandler(zkClient)
    }

    val updatedPartitionsBeingReassigned = controllerContext.partitionsBeingReassigned -- partitionsToBeRemoved

    info(s"Removing partitions $partitionsToBeRemoved from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment()
      // Ensure we detect future reassignments
      eventManager.put(PartitionReassignment)
    } else {
      val reassignment = updatedPartitionsBeingReassigned.mapValues(_.newReplicas)
      try zkClient.setOrCreatePartitionReassignment(reassignment)
      catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }

    controllerContext.partitionsBeingReassigned --= partitionsToBeRemoved
  }

  private def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                           isTriggeredByAutoRebalance : Boolean) {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info(s"Partition $partition completed preferred replica leader election. New leader is $preferredReplica")
      } else {
        warn(s"Partition $partition failed to complete preferred replica leader election to $preferredReplica. " +
          s"Leader is still $currentLeader")
      }
    }
    if (!isTriggeredByAutoRebalance) {
      zkClient.deletePreferredReplicaElection()
      // Ensure we detect future preferred replica leader elections
      eventManager.put(PreferredReplicaLeaderElection)
    }
  }

  private[controller] def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                                      callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  private[controller] def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition] = Set.empty[TopicPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch $epoch went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(successfulUpdates, _, failedUpdates) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch)
          if (successfulUpdates.contains(partition)) {
            val finalLeaderAndIsr = successfulUpdates(partition)
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(finalLeaderAndIsr, epoch))
            info(s"Updated leader epoch for partition $partition to ${finalLeaderAndIsr.leaderEpoch}")
            true
          } else if (failedUpdates.contains(partition)) {
            throw failedUpdates(partition)
          } else false
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicPartition, Seq[Int]]] =
      controllerContext.allPartitions.filterNot {
        tp => topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.map { tp =>
        (tp, controllerContext.partitionReplicaAssignment(tp) )
      }.toMap.groupBy { case (_, assignedReplicas) => assignedReplicas.head }

    debug(s"Preferred replicas by broker $preferredReplicasForTopicsByBrokers")

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.foreach { case (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica for broker $leaderBroker $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        topicsNotInPreferredReplica.keys.foreach { topicPartition =>
          // do this check only if the broker is live and there are no partitions being reassigned currently
          // and preferred replica election is not in progress
          if (controllerContext.isReplicaOnline(leaderBroker, topicPartition) &&
            controllerContext.partitionsBeingReassigned.isEmpty &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
            controllerContext.allTopics.contains(topicPartition.topic)) {
            onPreferredReplicaElection(Set(topicPartition), isTriggeredByAutoRebalance = true)
          }
        }
      }
    }
  }

  case object AutoPreferredReplicaLeaderElection extends ControllerEvent {

    def state = ControllerState.AutoLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      try {
        checkAndTriggerAutoLeaderRebalance()
      } finally {
        scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
      }
    }
  }

  case object UncleanLeaderElectionEnable extends ControllerEvent {

    def state = ControllerState.UncleanLeaderElectionEnable

    override def process(): Unit = {
      if (!isActive) return
      partitionStateMachine.triggerOnlinePartitionStateChange()
    }
  }

  case class ControlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {

    def state = ControllerState.ControlledShutdown

    override def process(): Unit = {
      val controlledShutdownResult = Try { doControlledShutdown(id) }
      controlledShutdownCallback(controlledShutdownResult)
    }

    private def doControlledShutdown(id: Int): Set[TopicPartition] = {
      if (!isActive) {
        throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
      }

      info(s"Shutting down broker $id")

      if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
        throw new BrokerNotAvailableException(s"Broker id $id does not exist.")

      controllerContext.shuttingDownBrokerIds.add(id)
      debug(s"All shutting down brokers: ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
      debug(s"Live brokers: ${controllerContext.liveBrokerIds.mkString(",")}")

      val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
        controllerContext.partitionReplicaAssignment(partition).size > 1 && controllerContext.partitionLeadershipInfo.contains(partition)
      }
      val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
        controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == id
      }
      partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Option(ControlledShutdownPartitionLeaderElectionStrategy))
      try {
        brokerRequestBatch.newBatch()
        partitionsFollowedByBroker.foreach { partition =>
          brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false,
            (_, _) => ())
        }
        brokerRequestBatch.sendRequestsToBrokers(epoch)
      } catch {
        case e: IllegalStateException =>
          handleIllegalState(e)
      }
      // If the broker is a follower, updates the isr in ZK and notifies the current leader
      replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
        PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
      def replicatedPartitionsBrokerLeads() = {
        trace(s"All leaders = ${controllerContext.partitionLeadershipInfo.mkString(",")}")
        controllerContext.partitionLeadershipInfo.filter {
          case (topicPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicPartition).size > 1
        }.keys
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  case class LeaderAndIsrResponseReceived(LeaderAndIsrResponseObj: AbstractResponse, brokerId: Int) extends ControllerEvent {

    def state = ControllerState.LeaderAndIsrResponseReceived

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val leaderAndIsrResponse = LeaderAndIsrResponseObj.asInstanceOf[LeaderAndIsrResponse]

      if (leaderAndIsrResponse.error != Errors.NONE) {
        stateChangeLogger.error(s"Received error in LeaderAndIsr response $leaderAndIsrResponse from broker $brokerId")
        return
      }

      val offlineReplicas = leaderAndIsrResponse.responses.asScala.collect {
        case (tp, error) if error == Errors.KAFKA_STORAGE_ERROR => tp
      }
      val onlineReplicas = leaderAndIsrResponse.responses.asScala.collect {
        case (tp, error) if error == Errors.NONE => tp
      }
      val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicPartition])
      val currentOfflineReplicas = previousOfflineReplicas -- onlineReplicas ++ offlineReplicas
      controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
      val newOfflineReplicas = currentOfflineReplicas -- previousOfflineReplicas

      if (newOfflineReplicas.nonEmpty) {
        stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
        onReplicasBecomeOffline(newOfflineReplicas.map(PartitionAndReplica(_, brokerId)))
      }
    }
  }

  case class TopicDeletionStopReplicaResponseReceived(stopReplicaResponseObj: AbstractResponse, replicaId: Int) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
      debug(s"Delete topic callback invoked for $stopReplicaResponse")
      val responseMap = stopReplicaResponse.responses.asScala
      val partitionsInError =
        if (stopReplicaResponse.error != Errors.NONE) responseMap.keySet
        else responseMap.filter { case (_, error) => error != Errors.NONE }.keySet
      val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
      // move all the failed replicas to ReplicaDeletionIneligible
      topicDeletionManager.failReplicaDeletion(replicasInError)
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError
        topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
      }
    }
  }

  case object Startup extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      //注册ControllerChangerHandler zookeeper监听器
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      //执行controller选举
      elect()
    }

  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        controllerContext.partitionLeadershipInfo.count { case (tp, leadershipInfo) =>
          !controllerContext.liveOrShuttingDownBrokerIds.contains(leadershipInfo.leaderAndIsr.leader) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
        }
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.allPartitions.count { topicPartition =>
          val replicas = controllerContext.partitionReplicaAssignment(topicPartition)
          val preferredReplica = replicas.head
          val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
          leadershipInfo.map(_.leaderAndIsr.leader != preferredReplica).getOrElse(false) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic)
        }
      }

    globalTopicCount = if (!isActive) 0 else controllerContext.allTopics.size

    globalPartitionCount = if (!isActive) 0 else controllerContext.partitionLeadershipInfo.size
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    onControllerResignation()
    activeControllerId = -1
    zkClient.deleteController()
  }
  //controller选举
  private def elect(): Unit = {
    val timestamp = time.milliseconds
    //获取当前Controllers所在broker的序号，如果controller不存在，就显示标记为-1
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    //如果controller已经选出来了，就返回
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }

    try {
      //zk上创建controller节点
      zkClient.checkedEphemeralCreate(ControllerZNode.path, ControllerZNode.encode(config.brokerId, timestamp))
      info(s"${config.brokerId} successfully elected as the controller")
      //将创建到controller节点的broker指定为新的controller
      activeControllerId = config.brokerId
      //执行当选controller的后续逻辑
      onControllerFailover()
    } catch {
      case _: NodeExistsException =>
        // If someone else has written the path, then
        activeControllerId = zkClient.getControllerId.getOrElse(-1)

        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}")
        else
          warn("A controller has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}", e2)
        triggerControllerMove()
    }
  }
  //broker节点变更事件
  case object BrokerChange extends ControllerEvent {
    override def state: ControllerState = ControllerState.BrokerChange

    override def process(): Unit = {
      //如果该broker不是controller，没有权限处理，直接返回
      if (!isActive) return
      //从zk上获取bbroker列表
      val curBrokers = zkClient.getAllBrokersInCluster.toSet
      val curBrokerIds = curBrokers.map(_.id)
      //获取controller当前保存的brokerID列表
      val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
      //获取新增broker列表
      val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
      //获取待移除broker列表
      val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds

      val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
      controllerContext.liveBrokers = curBrokers
      val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
      val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
      val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
      info(s"Newly added brokers: ${newBrokerIdsSorted.mkString(",")}, " +
        s"deleted brokers: ${deadBrokerIdsSorted.mkString(",")}, all live brokers: ${liveBrokerIdsSorted.mkString(",")}")
      //为每个新增broker创建与之连接的通道管理器和底层的请求发送线程（RequestSendThread）
      newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
      //为每个待移除的broker移除对应的配套资源
      deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      //为新增Broker执行更新Controller元数据h和Broker启动逻辑
      if (newBrokerIds.nonEmpty)
        //执行broker重启命令
        onBrokerStartup(newBrokerIdsSorted)
      if (deadBrokerIds.nonEmpty)
        //执行移除终止broker逻辑
        onBrokerFailure(deadBrokerIdsSorted)
    }
  }
  //一旦bbroker信息发生变更，该方法调用
  case class BrokerModifications(brokerId: Int) extends ControllerEvent {
    override def state: ControllerState = ControllerState.BrokerChange

    override def process(): Unit = {
      if (!isActive) return
      //获取目标broker的详细数据  包括每套监听器配置的主机名，端口号，及所使用的安全协议
      val newMetadata = zkClient.getBroker(brokerId)
      //从元数据缓存中h获取目标Broker的详细数据
      val oldMetadata = controllerContext.liveBrokers.find(_.id == brokerId)
      //如果两者不相等，说明broker数据发生了变更
      if (newMetadata.nonEmpty && oldMetadata.nonEmpty && newMetadata.map(_.endPoints) != oldMetadata.map(_.endPoints)) {
        info(s"Updated broker: ${newMetadata.get}")

        //更新元数据缓存，以及执行onBrokerUpdate方法处理Broker更新的逻辑
        val curBrokers = controllerContext.liveBrokers -- oldMetadata ++ newMetadata
        controllerContext.liveBrokers = curBrokers // Update broker metadata
        onBrokerUpdate(brokerId)
      }
    }
  }
    //处理如果topic更改的逻辑
  case object TopicChange extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      //首先从zk上获取所有主题
      val topics = zkClient.getAllTopicsInCluster.toSet
      //与元数据对比，找出新增主题列表
      val newTopics = topics -- controllerContext.allTopics
      //与元数据对比，找出已删除主题列表
      val deletedTopics = controllerContext.allTopics -- topics
      //使用zk上的主题列表更新元数据的缓存
      controllerContext.allTopics = topics
      //为新增主题注册分区变更监听器，分区变更监听器是监听主题分区变更的
      registerPartitionModificationsHandlers(newTopics.toSeq)
      //从zk中获取新增主题的分本分配情况
      val addedPartitionReplicaAssignment = zkClient.getReplicaAssignmentForTopics(newTopics)
      //清除元数据缓存中属于已删除主题的缓存项
      deletedTopics.foreach(controllerContext.removeTopic)
        //为新增主题更新元数据缓存中的副本分配条目
      addedPartitionReplicaAssignment.foreach {
        case (topicAndPartition, newReplicas) => controllerContext.updatePartitionReplicaAssignment(topicAndPartition, newReplicas)
      }
      info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
        s"[$addedPartitionReplicaAssignment]")
      //调整新增主题所有分区以及所属所有副本的运行状态为上线状态
      if (addedPartitionReplicaAssignment.nonEmpty)
        onNewPartitionCreation(addedPartitionReplicaAssignment.keySet)
    }
  }

  case object LogDirEventNotification extends ControllerEvent {
    override def state: ControllerState = ControllerState.LogDirChange

    override def process(): Unit = {
      if (!isActive) return
      val sequenceNumbers = zkClient.getAllLogDirEventNotifications
      try {
        val brokerIds = zkClient.getBrokerIdsFromLogDirEvents(sequenceNumbers)
        onBrokerLogDirFailure(brokerIds)
      } finally {
        // delete processed children
        zkClient.deleteLogDirEventNotifications(sequenceNumbers)
      }
    }
  }

  case class PartitionModifications(topic: String) extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicChange

    def restorePartitionReplicaAssignment(topic: String, newPartitionReplicaAssignment : immutable.Map[TopicPartition, Seq[Int]]): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment.filter(p =>
        existingPartitions.contains(p._1.partition.toString))

      zkClient.setTopicAssignment(topic, existingPartitionReplicaAssignment)
    }

    override def process(): Unit = {
      if (!isActive) return
      val partitionReplicaAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic))
      val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
        controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
      }
      if (topicDeletionManager.isTopicQueuedUpForDeletion(topic))
        if (partitionsToBeAdded.nonEmpty) {
          warn("Skipping adding partitions %s for topic %s since it is currently being deleted"
            .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))

          restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
        } else {
          // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
          info("Ignoring partition change during topic deletion as no new partitions are added")
        }
      else {
        if (partitionsToBeAdded.nonEmpty) {
          info(s"New partitions to be added $partitionsToBeAdded")
          partitionsToBeAdded.foreach { case (topicPartition, assignedReplicas) =>
            controllerContext.updatePartitionReplicaAssignment(topicPartition, assignedReplicas)
          }
          onNewPartitionCreation(partitionsToBeAdded.keySet)
        }
      }
    }
  }
  //删除topic监听器
  case object TopicDeletion extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicDeletion

    override def process(): Unit = {
      if (!isActive) return
      //从zk上获取待删除主题列表
      var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
      debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
      //找出不存在的主题列表
      val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
      if (nonExistentTopics.nonEmpty) {
        warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
        zkClient.deleteTopicDeletions(nonExistentTopics.toSeq)
      }
      topicsToBeDeleted --= nonExistentTopics
      //如果delete.topic.enable的参数设置为true
      if (config.deleteTopicEnable) {
        if (topicsToBeDeleted.nonEmpty) {
          info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic =>
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if (partitionReassignmentInProgress)
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
          }
          //将待删除主题插入到删除等待集合交由TopicDeletionManager处理
          topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      } else {  //不允许删除主题
        // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
        info(s"Removing $topicsToBeDeleted since delete topic is disabled")
        zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq)
      }
    }
  }

  case object PartitionReassignment extends ControllerEvent {
    override def state: ControllerState = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return

      // We need to register the watcher if the path doesn't exist in order to detect future reassignments and we get
      // the `path exists` check for free
      if (zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
        val partitionReassignment = zkClient.getPartitionReassignment

        // Populate `partitionsBeingReassigned` with all partitions being reassigned before invoking
        // `maybeTriggerPartitionReassignment` (see method documentation for the reason)
        partitionReassignment.foreach { case (tp, newReplicas) =>
          val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(KafkaController.this, eventManager,
            tp)
          controllerContext.partitionsBeingReassigned.put(tp, ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler))
        }

        maybeTriggerPartitionReassignment(partitionReassignment.keySet)
      }
    }
  }

  case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
    override def state: ControllerState = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
      // check if this partition is still being reassigned or not
      controllerContext.partitionsBeingReassigned.get(partition).foreach { reassignedPartitionContext =>
        val reassignedReplicas = reassignedPartitionContext.newReplicas.toSet
        zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
          case Some(leaderIsrAndControllerEpoch) => // check if new replicas have joined ISR
            val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
            val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
            if (caughtUpReplicas == reassignedReplicas) {
              // resume the partition reassignment process
              info(s"${caughtUpReplicas.size}/${reassignedReplicas.size} replicas have caught up with the leader for " +
                s"partition $partition being reassigned. Resuming partition reassignment")
              onPartitionReassignment(partition, reassignedPartitionContext)
            }
            else {
              info(s"${caughtUpReplicas.size}/${reassignedReplicas.size} replicas have caught up with the leader for " +
                s"partition $partition being reassigned. Replica(s) " +
                s"${(reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")} still need to catch up")
            }
          case None => error(s"Error handling reassignment of partition $partition to replicas " +
                         s"${reassignedReplicas.mkString(",")} as it was never created")
        }
      }
    }
  }

  case object IsrChangeNotification extends ControllerEvent {
    override def state: ControllerState = ControllerState.IsrChange

    override def process(): Unit = {
      if (!isActive) return
      val sequenceNumbers = zkClient.getAllIsrChangeNotifications
      try {
        val partitions = zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers)
        if (partitions.nonEmpty) {
          updateLeaderAndIsrCache(partitions)
          processUpdateNotifications(partitions)
        }
      } finally {
        // delete the notifications
        zkClient.deleteIsrChangeNotifications(sequenceNumbers)
      }
    }

    private def processUpdateNotifications(partitions: Seq[TopicPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }
  }

  case object PreferredReplicaLeaderElection extends ControllerEvent {
    override def state: ControllerState = ControllerState.ManualLeaderBalance

    override def process(): Unit = {
      if (!isActive) return

      // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
      // leader elections and we get the `path exists` check for free
      if (zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
        val partitions = zkClient.getPreferredReplicaElection
        val partitionsForTopicsToBeDeleted = partitions.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
        if (partitionsForTopicsToBeDeleted.nonEmpty) {
          error(s"Skipping preferred replica election for partitions $partitionsForTopicsToBeDeleted since the " +
            "respective topics are being deleted")
        }
        onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
      }
    }
  }
  //，ControllerChange事件仅执行卸任逻辑即
  case object ControllerChange extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      activeControllerId = zkClient.getControllerId.getOrElse(-1)
      if (wasActiveBeforeChange && !isActive) {
        onControllerResignation()
      }
    }
  }
  //relect事件除了卸任，还需要重新选举leader事件
  case object Reelect extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      activeControllerId = zkClient.getControllerId.getOrElse(-1)
      if (wasActiveBeforeChange && !isActive) {
        //执行卸任逻辑
        onControllerResignation()
      }
      //进行新一轮的ccontroller选举
      elect()
    }
  }

  case object RegisterBrokerAndReelect extends ControllerEvent {
    override def state: ControllerState = ControllerState.ControllerChange

    override def process(): Unit = {
      zkClient.registerBrokerInZk(brokerInfo)
      Reelect.process()
    }
  }

  // We can't make this a case object due to the countDownLatch field
  class Expire extends ControllerEvent {
    private val processingStarted = new CountDownLatch(1)
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      processingStarted.countDown()
      activeControllerId = -1
      onControllerResignation()
    }

    def waitUntilProcessingStarted(): Unit = {
      processingStarted.await()
    }
  }

}
//监听broker的数量变化
class BrokerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  override def handleChildChange(): Unit = {
    eventManager.put(controller.BrokerChange)   //仅仅是向事件队列写入BrokerChange事件
  }
}
//监听broker 的数据信息变更，包括broker配置信息
class BrokerModificationsHandler(controller: KafkaController, eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  override val path: String = BrokerIdZNode.path(brokerId)

  override def handleDataChange(): Unit = {
    eventManager.put(controller.BrokerModifications(brokerId))
  }
}
//监听主题数量创建和变更 ，脚本创建主题其实是往zk对应目录下写数据，然后本监听器会感知到，然后触发主题更新事件
class TopicChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.TopicChange)
}
//监听日志路径变更，一旦被触发，x需要获取sh受影响的broker列表，然后处理这些brokers上失效的日志路径。
class LogDirEventNotificationHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}
//  监听主题分区数据变更的监听器，比如新增加了副本，分区更换了leader副本
class PartitionModificationsHandler(controller: KafkaController, eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(controller.PartitionModifications(topic))
}
//   主题删除监听器  监听/admin/delete_topics的子节点数量变更，删除节点时，kafka的删除命令仅仅是在zk上创建 /admin/delete_topics/name节点，一旦监听到该节点创建，就会触发本监听器
class TopicDeletionHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  //zk： /admin/delete_topics路径
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.TopicDeletion)
}

class PartitionReassignmentHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(controller.PartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(controller: KafkaController, eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(controller.PartitionReassignmentIsrChange(partition))
}
//监听ISR副本集合变更， 一旦被触发，就需要获取ISRf发生变更的分区列表，然后更新Controller端对应的Leaderh和ISRh缓存元数据
class IsrChangeNotificationHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}
//监听分区副本重分配
class PreferredReplicaElectionHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(controller.PreferredReplicaLeaderElection)
}

//     监听/controller节点变更的  包括节点创建，删除，数据变更
class ControllerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  //zookeeper中 controller节点路径  即 /controller
  override val path: String = ControllerZNode.path
  //监听 /controller节点创建事件
  override def handleCreation(): Unit = eventManager.put(controller.ControllerChange)
  //监听 /controller节点被删除事件
  override def handleDeletion(): Unit = eventManager.put(controller.Reelect)
  //监听 /controller节点数据变更事件
  override def handleDataChange(): Unit = eventManager.put(controller.ControllerChange)
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       val reassignIsrChangeHandler: PartitionReassignmentIsrChangeHandler) {

  def registerReassignIsrChangeHandler(zkClient: KafkaZkClient): Unit =
    zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)

  def unregisterReassignIsrChangeHandler(zkClient: KafkaZkClient): Unit =
    zkClient.unregisterZNodeChangeHandler(reassignIsrChangeHandler.path)

}

case class PartitionAndReplica(topicPartition: TopicPartition, replica: Int) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition

  override def toString: String = {
    s"[Topic=$topic,Partition=$partition,Replica=$replica]"
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats extends KafkaMetricsGroup {
  //统计Coltroller每秒发生的UnClean leader选举次数
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
  //统计所有controller状态的速率和时间信息  ms
  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}
//controllers事件，也就是事件队列总被处理的对象，ControllerEventThread专门处理不同类型的ControllerEvent
sealed trait ControllerEvent {
  val enqueueTimeMs: Long = Time.SYSTEM.milliseconds()

  def state: ControllerState  //controller在处理具体事件时，会对状态进行相应的变更。
  //处理本ControllerEvent事件
  def process(): Unit
}
