/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

//定义各种Controller事件以及这些事件被处理的代码
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}
class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit) extends KafkaMetricsGroup {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  //保存事件的队列
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)
  private val time = Time.SYSTEM

  private val eventQueueTimeHist = newHistogram("EventQueueTimeMs")

  newGauge(
    "EventQueueSize",
    new Gauge[Int] {
      def value: Int = {
        queue.size()
      }
    }
  )


  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.clear()
    put(event)
  }

  //专属的事件处理线程，唯一d的作用是c处理不同种类的ControllerEvent
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      //从事件队列种获取事件，否则等待
      queue.take() match {
          //如果s是关闭线程操作，什么都不用做。关闭线程由外部完成。
        case KafkaController.ShutdownEventThread => initiateShutdown()
        case controllerEvent =>
          _state = controllerEvent.state
          //更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - controllerEvent.enqueueTimeMs)

          try {
            //处理事件，同时计算处理速率
            rateAndTimeMetrics(state).time {
              controllerEvent.process()
            }
          } catch {
            case e: Throwable => error(s"Error processing event $controllerEvent", e)
          }

          try eventProcessedListener(controllerEvent)
          catch {
            case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
