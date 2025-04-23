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

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**https://weishihuai.blog.csdn.net/article/details/135871624
 * Abstract nacos task execute engine.
 *
 * @author xiweng.yy
 */
public abstract class AbstractNacosTaskExecuteEngine<T extends NacosTask> implements NacosTaskExecuteEngine<T> {

	private final Logger log;
	/**
	 * 对处理类NacosTaskProcessor的缓存 key: Service服务 value: NacosTaskProcessor处理类
	 */
	private final ConcurrentHashMap<Object, NacosTaskProcessor> taskProcessors = new ConcurrentHashMap<>();
	/**
	 * 默认的处理类。缓存中不存在的话，就用这个类去处理
	 */
	private NacosTaskProcessor defaultTaskProcessor;

	public AbstractNacosTaskExecuteEngine(Logger logger) {
		this.log = null != logger ? logger : LoggerFactory.getLogger(AbstractNacosTaskExecuteEngine.class.getName());
	}

	@Override
	public void addProcessor(Object key, NacosTaskProcessor taskProcessor) {
		 // 添加处理类，没有的时候才添加
		taskProcessors.putIfAbsent(key, taskProcessor);
	}

	@Override
	public void removeProcessor(Object key) {
		taskProcessors.remove(key);
	}

	@Override
	public NacosTaskProcessor getProcessor(Object key) {
		 // 如果缓存中能找到对应的处理器类，则直接返回。否则使用默认的处理类
		return taskProcessors.containsKey(key) ? taskProcessors.get(key) : defaultTaskProcessor;
	}

	@Override
	public Collection<Object> getAllProcessorKey() {
		return taskProcessors.keySet();
	}

	@Override
	public void setDefaultTaskProcessor(NacosTaskProcessor defaultTaskProcessor) {
		this.defaultTaskProcessor = defaultTaskProcessor;
	}

	protected Logger getEngineLog() {
		return log;
	}
}
