/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nacos delay task execute engine.
 *
 * @author xiweng.yy
 */
public class NacosDelayTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractDelayTask> {

	private final ScheduledExecutorService processingExecutor;

	protected final ConcurrentHashMap<Object, AbstractDelayTask> tasks;

	protected final ReentrantLock lock = new ReentrantLock();

	public NacosDelayTaskExecuteEngine(String name) {
		this(name, null);
	}

	public NacosDelayTaskExecuteEngine(String name, Logger logger) {
		this(name, 32, logger, 100L);
	}

	public NacosDelayTaskExecuteEngine(String name, Logger logger, long processInterval) {
		this(name, 32, logger, processInterval);
	}

	public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger) {
		this(name, initCapacity, logger, 100L);
	}

	public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger, long processInterval) {
		super(logger);
		// 初始化任务队列
		tasks = new ConcurrentHashMap<>(initCapacity);
		// 创建定时任务的线程池
		processingExecutor = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory(name));
		// 在指定的初始延迟时间(100毫秒)后开始执行任务，并按固定的时间间隔周期性(100毫秒)地执行任务。
		// 默认延时100毫秒执行ProcessRunnable，然后每隔100毫秒周期性执行ProcessRunnable
		processingExecutor.scheduleWithFixedDelay(new ProcessRunnable(), processInterval, processInterval,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public int size() {
		lock.lock();
		try {
			return tasks.size();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		lock.lock();
		try {
			return tasks.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AbstractDelayTask removeTask(Object key) {
		lock.lock();
		try {
			AbstractDelayTask task = tasks.get(key);
			if (null != task && task.shouldProcess()) {
				return tasks.remove(key);
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Collection<Object> getAllTaskKeys() {
		Collection<Object> keys = new HashSet<Object>();
		lock.lock();
		try {
			keys.addAll(tasks.keySet());
		} finally {
			lock.unlock();
		}
		return keys;
	}

	@Override
	public void shutdown() throws NacosException {
		tasks.clear();
		processingExecutor.shutdown();
	}
	// 代码解释完了，解释下整体的流程
	//
	// 通过delayTaskEngine添加到tasks(这是一个ConcurrentHashMap)中
	// delayTaskEngine有个单线程的延迟任务线程池processingExecutor在不断的获取任务
	// 因为delayTaskEngine是一个PushDelayTaskExecuteEngine，
	// 在构造方法中只添加了默认的处理类PushDelayTaskProcessor，所以都是由PushDelayTaskProcessor中的process(NacosTask
	// task)执行。
	// 这个process(NacosTask task)又调用了
	// NamingExecuteTaskDispatcher.getInstance() .dispatchAndExecuteTask(service,
	// new PushExecuteTask(service, executeEngine, pushDelayTask))。

	@Override
	public void addTask(Object key, AbstractDelayTask newTask) {
		lock.lock();
		try {
			// 根据service拿取对应任务，刚开始一定不存在
			AbstractDelayTask existTask = tasks.get(key);
			if (null != existTask) {
				// 服务存在的话，则需要合并任务，其实就是合并多个任务，一起执行
				newTask.merge(existTask);
			}
			// 将任务放到tasks里面，一定有地方处理他
			tasks.put(key, newTask);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * process tasks in execute engine.
	 */
	// 处理我们任务的引擎
	protected void processTasks() {
		// 获取所有的task 的keys
		Collection<Object> keys = getAllTaskKeys();
		for (Object taskKey : keys) {
			AbstractDelayTask task = removeTask(taskKey);
			if (null == task) {
				continue;
			}
			// taskKey示例值: Service{namespace='public', group='DEFAULT_GROUP',
			// name='discovery-provider', ephemeral=true, revision=0}
			// 找到处理类
			NacosTaskProcessor processor = getProcessor(taskKey);
			if (null == processor) {
				getEngineLog().error("processor not found for task, so discarded. " + task);
				continue;
			}
			try {
				// ReAdd task if process failed
				// key:处理task任务
				if (!processor.process(task)) {
					// 处理失败的话，重新入队（即重试）
					retryFailedTask(taskKey, task);
				}
			} catch (Throwable e) {
				getEngineLog().error("Nacos task execute error ", e);
				retryFailedTask(taskKey, task);
			}
		}
	}

	private void retryFailedTask(Object key, AbstractDelayTask task) {
		task.setLastProcessTime(System.currentTimeMillis());
		addTask(key, task);
	}

	private class ProcessRunnable implements Runnable {

		@Override
		public void run() {
			try {
				// 处理我们的任务
				processTasks();
			} catch (Throwable e) {
				getEngineLog().error(e.toString(), e);
			}
		}
	}
}
