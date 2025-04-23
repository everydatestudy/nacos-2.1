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

package com.alibaba.nacos.naming.core.v2.service.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.InstanceMetadataEvent;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.ClientOperationService;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.springframework.stereotype.Component;

/**
 * Operation service for ephemeral clients and services.
 *
 * @author xiweng.yy
 */
@Component("ephemeralClientOperationService")
public class EphemeralClientOperationServiceImpl implements ClientOperationService {

	private final ClientManager clientManager;

	public EphemeralClientOperationServiceImpl(ClientManagerDelegate clientManager) {
		this.clientManager = clientManager;
	}

	@Override
	public void registerInstance(Service service, Instance instance, String clientId) {
		// // ServiceManager.getInstance()使用饿汉式单例返回一个ServiceManager对象
		// getSingleton从缓存singletonRepository中获取一个单例Service,
		// 已存在的时候直接从缓存获取（注意Service的equals和hasCode方法）
		Service singleton = ServiceManager.getInstance().getSingleton(service);
		// 判断获取到的service是否是临时实例，如果不是，则报错，因为当前的service（EphemeralClientOperationServiceImpl）就是处理临时实例的
		if (!singleton.isEphemeral()) {
			throw new NacosRuntimeException(NacosException.INVALID_PARAM,
					String.format("Current service %s is persistent service, can't register ephemeral instance.",
							singleton.getGroupedServiceName()));
		}
		// grpc给我封装的对象
		Client client = clientManager.getClient(clientId);
		// 判断Client是否合法：
		// 1、client是否为空，为空代表客户端已经断开连接，非法
		// 2、client是否为临时的，如果非临时的连接，非法，直接返回
		if (!clientIsLegal(client, clientId)) {
			return;
		}
		// 这里将instance转化为InstancePublishInfo, 包含一些属性，如实例IP、实例ID、端口号等等
		InstancePublishInfo instanceInfo = getPublishInfo(instance);

		// 负责存储当前客户端服务注册表，也就是 service和instance的关系。
		// 1.客户端将自己注册到了服务器端的ClientManager中；
		// 2. Client客户端内部有一个Map: ConcurrentHashMap<Service,
		// InstancePublishInfo>publishers. 即发布者列表
		// 3. 将实例信息放入发布者列表中（ key: Service 、 value: 实例发布信息）
		client.addServiceInstance(singleton, instanceInfo);
		// 设置客户端最后更新时间为当前时间
		client.setLastUpdatedTime();
		 // 发布客户端注册事件通知订阅者,
		// Nacos使用了发布-订阅模式来处理.
		// 简单理解就是，事件发布器发布相应的事件，然后对事件感兴趣的订阅者就会进行相应的处理，解耦
		NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
		 // 发布实例元数据事件通知订阅者
		InstanceMetadataEvent metadataEvent = new MetadataEvent.InstanceMetadataEvent(singleton,
				instanceInfo.getMetadataId(), false);
		NotifyCenter.publishEvent(metadataEvent);
	}

	@Override
	public void deregisterInstance(Service service, Instance instance, String clientId) {
		if (!ServiceManager.getInstance().containSingleton(service)) {
			Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", service);
			return;
		}
		Service singleton = ServiceManager.getInstance().getSingleton(service);
		Client client = clientManager.getClient(clientId);
		if (!clientIsLegal(client, clientId)) {
			return;
		}
		InstancePublishInfo removedInstance = client.removeServiceInstance(singleton);
		client.setLastUpdatedTime();
		if (null != removedInstance) {
			NotifyCenter.publishEvent(new ClientOperationEvent.ClientDeregisterServiceEvent(singleton, clientId));
			NotifyCenter.publishEvent(
					new MetadataEvent.InstanceMetadataEvent(singleton, removedInstance.getMetadataId(), true));
		}
	}

	@Override
	public void subscribeService(Service service, Subscriber subscriber, String clientId) {
		Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
		// 获取客户端
		Client client = clientManager.getClient(clientId);
		if (!clientIsLegal(client, clientId)) {
			return;
		}
		// 把订阅者和服务进行关系绑定
		client.addServiceSubscriber(singleton, subscriber);
		client.setLastUpdatedTime();
		// 发布订阅事件
		NotifyCenter.publishEvent(new ClientOperationEvent.ClientSubscribeServiceEvent(singleton, clientId));
	}

	@Override
	public void unsubscribeService(Service service, Subscriber subscriber, String clientId) {
		Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
		Client client = clientManager.getClient(clientId);
		if (!clientIsLegal(client, clientId)) {
			return;
		}
		client.removeServiceSubscriber(singleton);
		client.setLastUpdatedTime();
		NotifyCenter.publishEvent(new ClientOperationEvent.ClientUnsubscribeServiceEvent(singleton, clientId));
	}

	private boolean clientIsLegal(Client client, String clientId) {
		if (client == null) {
			Loggers.SRV_LOG.warn("Client connection {} already disconnect", clientId);
			return false;
		}
		if (!client.isEphemeral()) {
			Loggers.SRV_LOG.warn("Client connection {} type is not ephemeral", clientId);
			return false;
		}
		return true;
	}
}
