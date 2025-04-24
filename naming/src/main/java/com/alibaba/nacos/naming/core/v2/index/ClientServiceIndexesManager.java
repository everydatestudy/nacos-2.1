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

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client and service index manager.
 *
 * @author xiweng.yy
 */
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {

	private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();

	private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();

	public ClientServiceIndexesManager() {
		// 向NotifyCenter中注册自己
		// NamingEventPublisherFactory.getInstance()也是用到了单例，返回了NamingEventPublisherFactory
		NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
	}

	public Collection<String> getAllClientsRegisteredService(Service service) {
		return publisherIndexes.containsKey(service) ? publisherIndexes.get(service) : new ConcurrentHashSet<>();
	}

	public Collection<String> getAllClientsSubscribeService(Service service) {
		// 从订阅列表中获取所有订阅者ID
		return subscriberIndexes.containsKey(service) ? subscriberIndexes.get(service) : new ConcurrentHashSet<>();
	}

	public Collection<Service> getSubscribedService() {
		return subscriberIndexes.keySet();
	}

	/**
	 * Clear the service index without instances.
	 *
	 * @param service The service of the Nacos.
	 */
	public void removePublisherIndexesByEmptyService(Service service) {
		if (publisherIndexes.containsKey(service) && publisherIndexes.get(service).isEmpty()) {
			publisherIndexes.remove(service);
		}
	}

	/**
	 * 能处理那些事件，我们这里都已经添加好了
	 * 
	 * @return
	 */
	@Override
	public List<Class<? extends Event>> subscribeTypes() {
		List<Class<? extends Event>> result = new LinkedList<>();
		// 注册事件
		result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);
		// 注销事件
		result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);
		// 订阅事件
		result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);
		// 取消订阅事件
		result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);
		result.add(ClientEvent.ClientDisconnectEvent.class);
		return result;
	}

	/**
	 * 进行事件处理
	 * 
	 * @param event {@link Event}
	 */
	@Override
	public void onEvent(Event event) {
		if (event instanceof ClientEvent.ClientDisconnectEvent) {
			// 处理过期连接
			handleClientDisconnect((ClientEvent.ClientDisconnectEvent) event);
		} else if (event instanceof ClientOperationEvent) {
			// 注册、注销、订阅、取消订阅父类都是ClientOperationEvent ，所以他是在这里进行处理的
			handleClientOperation((ClientOperationEvent) event);
		}
	}

	private void handleClientDisconnect(ClientEvent.ClientDisconnectEvent event) {
		Client client = event.getClient();
		for (Service each : client.getAllSubscribeService()) {
			 // 从订阅者列表中移除所有服务对这个客户端的引用
	        // private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();
	        // key: Service      value: 客户端ID集合
			removeSubscriberIndexes(each, client.getClientId());
		}
		for (Service each : client.getAllPublishedService()) {
			// 移除注册表数据
			removePublisherIndexes(each, client.getClientId());
		}
	}

	private void handleClientOperation(ClientOperationEvent event) {
		Service service = event.getService();
		String clientId = event.getClientId();
		if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
			// 服务注册 处理ClientRegisterServiceEvent事件（服务注册）
			addPublisherIndexes(service, clientId);
		} else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
			// 注销服务
			removePublisherIndexes(service, clientId);
		} else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
			// 服务订阅
			addSubscriberIndexes(service, clientId);
		} else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
			// 取消服务
			removeSubscriberIndexes(service, clientId);
		}
	}

	private void addPublisherIndexes(Service service, String clientId) {
		// 这map key:服务 value: set集合，这个set里面是对应我们客户端Id
		// publisherIndexes 服务注册表
		publisherIndexes.computeIfAbsent(service, (key) -> new ConcurrentHashSet<>());
		publisherIndexes.get(service).add(clientId);
		// 上面注册完毕，后发布服务变更事件
		NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
	}

	private void removePublisherIndexes(Service service, String clientId) {
		if (!publisherIndexes.containsKey(service)) {
			return;
		}
		publisherIndexes.get(service).remove(clientId);
		NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
	}

	private void addSubscriberIndexes(Service service, String clientId) {
		// 这里是订阅列表 这里服务注册列表类似
		subscriberIndexes.computeIfAbsent(service, (key) -> new ConcurrentHashSet<>());
		// Fix #5404, Only first time add need notify event.
		if (subscriberIndexes.get(service).add(clientId)) {
			NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
		}
	}

	private void removeSubscriberIndexes(Service service, String clientId) {
		if (!subscriberIndexes.containsKey(service)) {
			return;
		}
		subscriberIndexes.get(service).remove(clientId);
		if (subscriberIndexes.get(service).isEmpty()) {
			subscriberIndexes.remove(service);
		}
	}
}
