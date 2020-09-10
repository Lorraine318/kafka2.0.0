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
package kafka.utils.timer

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */
@nonthreadsafe
//时间轮   包含多个TimerTaskList
private[timer] class TimingWheel(tickMs: Long, //滴答一次的时长 ，就是向前推进一格的时间，在Kafka中，第一层时间轮的ticMs被固定为1毫秒，也就是说向前推动一格bucket的时长就是1毫秒
                                 wheelSize: Int, //每一层时间轮上的Bucket数量，第一层的bucket数量是20
                                 startMs: Long, //时间轮对象被创建时的起始时间戳
                                 taskCounter: AtomicInteger, //这一层时间轮上的总定时任务数
                                 queue: DelayQueue[TimerTaskList] //将所有bucket按照该过期时间排序的延迟队列，随着时间推移，kafka依靠这个队列获取那些过期的bucket,并清除他们
                                ) {

  private[this] val interval = tickMs * wheelSize  //这层时间轮总时长，滴答时长* 该层bucket数量，第一次也就是20毫秒，由于下一层的时间轮滴答时长就是上一层的总时长，第二层也就是抵达市场20毫秒，20层，400毫秒
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) } //时间轮下的所有bucket对象

  private[this] var currentTime = startMs - (startMs % tickMs) // 假设滴答时长20毫秒，当前时间戳123毫秒，那么currentTime就是123-123%20 =120

  //创建上层时间轮的， 当有新定时任务到达时，会尝试将其加入第一层时间轮，如果第一层的interval总时长无法容纳下该定时任务，就现场创建并配置好第二层时间轮，并尝试放入，如果依然无法容纳，继续创建
  @volatile private[this] var overflowWheel: TimingWheel = null

  //创建上层时间轮
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      //只有之前没有创建上层时间轮方法才会继续
      if (overflowWheel == null) {
        //创建新的时间轮实例
        //滴答时长tickMs等于下层时间轮总时长
        //每层轮子都是相同的
        overflowWheel = new TimingWheel(
          tickMs = interval,
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    //获取定时任务的过期时间戳
    val expiration = timerTaskEntry.expirationMs
    //如果该任务已然被取消，则无需添加，直接返回
    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
      //如果该任务超时时间已过期
    } else if (expiration < currentTime + tickMs) {
      // Already expired
      false
      //如果该任务超时时间在本层时间轮覆盖时间范围内
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket
      val virtualId = expiration / tickMs
      //计算要被放入到那个Bucket中
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      //添加到bucket中
      bucket.add(timerTaskEntry)

     //设置bucket过期时间
      //如果该时间变更过，说明bucket是新建或被重用，将其加回到DelayQueue中
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
      //本层时间轮无法容纳该任务，交由上层时间轮处理
    } else {
      // 按需创建时间轮
      if (overflowWheel == null) addOverflowWheel()
      //加入到上层时间轮中
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  def advanceClock(timeMs: Long): Unit = {
    //向前驱动到的时点要超过bucket的时间范围，才是有意义的推进，否则什么也不做
    //更新当前时间到下一个bucket的起始点
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // 同时尝试为上一层时间轮做向前推进操作
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
