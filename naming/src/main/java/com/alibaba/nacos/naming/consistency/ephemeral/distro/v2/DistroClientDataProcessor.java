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

package com.alibaba.nacos.naming.consistency.ephemeral.distro.v2;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncData;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncDatumSnapshot;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.upgrade.UpgradeJudgement;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Distro processor for v2.
 *
 * @author xiweng.yy
 */
public class DistroClientDataProcessor extends SmartSubscriber implements DistroDataStorage, DistroDataProcessor {

	public static final String TYPE = "Nacos:Naming:v2:ClientData";

	private final ClientManager clientManager;

	private final DistroProtocol distroProtocol;

	private final UpgradeJudgement upgradeJudgement;

	private volatile boolean isFinishInitial;

	public DistroClientDataProcessor(ClientManager clientManager, DistroProtocol distroProtocol,
			UpgradeJudgement upgradeJudgement) {
		this.clientManager = clientManager;
		this.distroProtocol = distroProtocol;
		this.upgradeJudgement = upgradeJudgement;
		NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
	}

	@Override
	public void finishInitial() {
		isFinishInitial = true;
	}

	@Override
	public boolean isFinishInitial() {
		return isFinishInitial;
	}

	@Override
	public List<Class<? extends Event>> subscribeTypes() {
		List<Class<? extends Event>> result = new LinkedList<>();
		result.add(ClientEvent.ClientChangedEvent.class);
		result.add(ClientEvent.ClientDisconnectEvent.class);
		result.add(ClientEvent.ClientVerifyFailedEvent.class);
		return result;
	}

	@Override
	public void onEvent(Event event) {
		// 只有集群模式才有效，单机模式启动的Nacos，不会执行同步操作
		if (EnvUtil.getStandaloneMode()) {
			return;
		}
		if (!upgradeJudgement.isUseGrpcFeatures()) {
			return;
		}
		// 同步到验证失败的服务节点上
		if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
			syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);
		} else {
			// 同步数据给其它节点
			syncToAllServer((ClientEvent) event);
		}
	}

	private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
		Client client = clientManager.getClient(event.getClientId());
		if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
			return;
		}
		DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
		// Verify failed data should be sync directly.
		// 验证是否应直接同步失败的数据，依然是被包装，再通过rpc请求到对应的服务节点
		distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
	}

	/**
	 * 进群数据同步
	 * 
	 * @param event
	 */
	private void syncToAllServer(ClientEvent event) {
		Client client = event.getClient();
		// Only ephemeral data sync by Distro, persist client should sync by raft.
		/**
		 * 判断客户端是否为空，是否是临时实例，判断是否是负责节点
		 * 
		 * 临时实例，使用distro协议同步； 持久实例：使用raft协议同步
		 */
		if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
			return;
		}
		/**
		 * 数据删除或者变更的情况下进行同步
		 */
		if (event instanceof ClientEvent.ClientDisconnectEvent) {
			DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
			distroProtocol.sync(distroKey, DataOperation.DELETE);
		} else if (event instanceof ClientEvent.ClientChangedEvent) {
			DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
			distroProtocol.sync(distroKey, DataOperation.CHANGE);
		}
	}

	@Override
	public String processType() {
		return TYPE;
	}

	@Override
	public boolean processData(DistroData distroData) {
		switch (distroData.getType()) {
		case ADD:
		case CHANGE:
			ClientSyncData clientSyncData = ApplicationUtils.getBean(Serializer.class)
					.deserialize(distroData.getContent(), ClientSyncData.class);
			// 新增修改实例
			handlerClientSyncData(clientSyncData);
			return true;
		case DELETE:
			String deleteClientId = distroData.getDistroKey().getResourceKey();
			Loggers.DISTRO.info("[Client-Delete] Received distro client sync data {}", deleteClientId);
			// 删除实例
			clientManager.clientDisconnected(deleteClientId);
			return true;
		default:
			return false;
		}
	}

	private void handlerClientSyncData(ClientSyncData clientSyncData) {
		Loggers.DISTRO.info("[Client-Add] Received distro client sync data {}", clientSyncData.getClientId());
		// 同步客户端信息
		clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes());
		Client client = clientManager.getClient(clientSyncData.getClientId());
		// 更新信息，发送注册时间
		upgradeClient(client, clientSyncData);
	}

	private void upgradeClient(Client client, ClientSyncData clientSyncData) {
		// 当前处理的远端节点中的数据集合
		// 获取所有的namespace
		List<String> namespaces = clientSyncData.getNamespaces();
		// 获取所有的groupNames
		List<String> groupNames = clientSyncData.getGroupNames();
		// 获取所有的serviceNames
		List<String> serviceNames = clientSyncData.getServiceNames();
		// 获取所有的instance
		List<InstancePublishInfo> instances = clientSyncData.getInstancePublishInfos();
		// 已同步的服务集合
		Set<Service> syncedService = new HashSet<>();

		// ①
		for (int i = 0; i < namespaces.size(); i++) {
			// 从获取的数据中构建一个Service对象
			Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
			Service singleton = ServiceManager.getInstance().getSingleton(service);
			// 标记此service已被处理
			syncedService.add(singleton);
			// 获取当前的实例
			InstancePublishInfo instancePublishInfo = instances.get(i);
			// 判断是否已经包含当前实例
			if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) {
				// 不包含则添加
				client.addServiceInstance(singleton, instancePublishInfo);
				// 当前节点发布服务注册事件
				NotifyCenter.publishEvent(
						new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
			}
		}
		// 若当前client内部已发布的service不在本次同步的列表内，说明已经过时了，要删掉
		for (Service each : client.getAllPublishedService()) {
			if (!syncedService.contains(each)) {
				client.removeServiceInstance(each);
				// 发布客户端下线事件
				NotifyCenter.publishEvent(
						new ClientOperationEvent.ClientDeregisterServiceEvent(each, client.getClientId()));
			}
		}
	}

	@Override
	public boolean processVerifyData(DistroData distroData, String sourceAddress) {
		  // 对数据进行反序列化为DistroClientVerifyInfo对象
		DistroClientVerifyInfo verifyData = ApplicationUtils.getBean(Serializer.class)
				.deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
		if (clientManager.verifyClient(verifyData.getClientId())) {
			return true;
		}
		Loggers.DISTRO.info("client {} is invalid, get new client from {}", verifyData.getClientId(), sourceAddress);
		return false;
	}

	@Override
	public boolean processSnapshot(DistroData distroData) {
		// 反序列化获取的DistroData为ClientSyncDatumSnapshot
		ClientSyncDatumSnapshot snapshot = ApplicationUtils.getBean(Serializer.class)
				.deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
		// 处理结果集，这里将返回远程节点负责的所有client以及client下面的service、instance信息
		for (ClientSyncData each : snapshot.getClientSyncDataList()) {
			handlerClientSyncData(each);
		}
		return true;
	}

	// 从Client管理器中获取指定Client
	@Override
	public DistroData getDistroData(DistroKey distroKey) {
		Client client = clientManager.getClient(distroKey.getResourceKey());
		if (null == client) {
			return null;
		}
		byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(client.generateSyncData());
		return new DistroData(distroKey, data);
	}

	@Override
	public DistroData getDatumSnapshot() {
		// 从Client管理器中获取所有Client
		List<ClientSyncData> datum = new LinkedList<>();
		for (String each : clientManager.allClientId()) {
			Client client = clientManager.getClient(each);
			if (null == client || !client.isEphemeral()) {
				continue;
			}
			datum.add(client.generateSyncData());
		}
		ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
		snapshot.setClientSyncDataList(datum);
		byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(snapshot);
		return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
	}

	// 在当前节点执行Nacos:Naming:v2:ClientData类型数据的验证任务时，
	// 它只会向集群中的其他节点发送自己负责的，且未被移除的数据。
	@Override
	public List<DistroData> getVerifyData() {
		List<DistroData> result = new LinkedList<>();
		// 遍历当前节点缓存的所有client
		for (String each : clientManager.allClientId()) {
			// 对每个本机所管理的注册客户端进行处理
			Client client = clientManager.getClient(each);
			if (null == client || !client.isEphemeral()) {
				// 空的或者是非临时性的节点，不处理
				continue;
			}
			// 是本机负责的Client才进行处理
			if (clientManager.isResponsibleClient(client)) {
				// TODO add revision for client.
				// 需要验证的数据就是每个节点的clientId和revision
				DistroClientVerifyInfo verifyData = new DistroClientVerifyInfo(client.getClientId(), 0);
				DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
				DistroData data = new DistroData(distroKey,
						ApplicationUtils.getBean(Serializer.class).serialize(verifyData));
				data.setType(DataOperation.VERIFY);
				result.add(data);
			}
		}
		return result;
	}
}
