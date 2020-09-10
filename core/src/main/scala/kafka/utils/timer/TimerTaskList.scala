/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

@threadsafe
//每个bucket中的双向链表，用来保存延迟请求    包含多个TimerTaskEntry
private[timer] class TimerTaskList(taskCounter: AtomicInteger  //用于标识当前这个链表中的总定时任务数
                                  ) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  //root是起始元素，不保存真实的TimerTaskEntry
  //链表元素
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root
  //表示这个bucket的过期时间戳
  private[this] val expiration = new AtomicLong(-1L)

  //原子性设置了过期时间戳，而且会判断旧值与新的过期时间戳进行比较，看是否相同
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask) => Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // 将给定的定时任务插入到链表中
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
     //在添加之前尝试移除该定时任务，保证该任务没有在其他链表中
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            //把timerTaskEntry 添加到链表末尾
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  //移除定时任务
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  def flush(f: (TimerTaskEntry) => Unit): Unit = {
    synchronized {
      //找到链表第一个元素
      var head = root.next
      //开始遍历链表
      while (head ne root) {
        //移除遍历到的链表元素
        remove(head)
        //执行传入参数的f逻辑
        f(head)
        head = root.next
      }
      //清空过期时间设置
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {

    val other = d.asInstanceOf[TimerTaskList]

    if (getExpiration < other.getExpiration) -1
    else if (getExpiration > other.getExpiration) 1
    else 0
  }

}

//双向链表中保存的链表元素，每个元素中TimerTaskEntry保存的延时任务 TimerTask     一个TimerTaskEntry 一个TimerTask
private[timer] class TimerTaskEntry(val timerTask: TimerTask,
                                    val expirationMs: Long  //定时任务的过期时间
                                   ) extends Ordered[TimerTaskEntry] {

  @volatile
  var list: TimerTaskList = null //绑定的Bucket链表实例   因为kafka的延时请求可能会被其他线程从一个链表搬移到另一个链表中  所以为了保证必要的内存可见性
  var next: TimerTaskEntry = null //next指针
  var prev: TimerTaskEntry = null //prev上一个指针

  //关联给定的定时任务
  if (timerTask != null) timerTask.setTimerTaskEntry(this)
    //判断关联的定时任务是否被取消了
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }
  //从bucket链表中移除自己
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    this.expirationMs compare that.expirationMs
  }
}

