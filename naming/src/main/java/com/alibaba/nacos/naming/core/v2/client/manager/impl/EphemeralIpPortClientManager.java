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

package com.alibaba.nacos.naming.core.v2.client.manager.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactoryHolder;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.healthcheck.heartbeat.ClientBeatUpdateTask;
import com.alibaba.nacos.naming.misc.ClientConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingExecuteTaskDispatcher;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The manager of {@code IpPortBasedClient} and ephemeral.
 *
 * @author xiweng.yy
 */
@Component("ephemeralIpPortClientManager")
public class EphemeralIpPortClientManager implements ClientManager {

	private final ConcurrentMap<String, IpPortBasedClient> clients = new ConcurrentHashMap<>();

	private final DistroMapper distroMapper;

	private final ClientFactory<IpPortBasedClient> clientFactory;

	public EphemeralIpPortClientManager(DistroMapper distroMapper, SwitchDomain switchDomain) {
		this.distroMapper = distroMapper;
		GlobalExecutor.scheduleExpiredClientCleaner(new ExpiredClientCleaner(this, switchDomain), 0,
				Constants.DEFAULT_HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
		clientFactory = ClientFactoryHolder.getInstance().findClientFactory(ClientConstants.EPHEMERAL_IP_PORT);
	}

	@Override
	public boolean clientConnected(String clientId, ClientAttributes attributes) {
		return clientConnected(clientFactory.newClient(clientId, attributes));
	}

	@Override
	public boolean clientConnected(final Client client) {
		// 构建新的实例的客户端对象
		clients.computeIfAbsent(client.getClientId(), s -> {
			Loggers.SRV_LOG.info("Client connection {} connect", client.getClientId());
			IpPortBasedClient ipPortBasedClient = (IpPortBasedClient) client;
			ipPortBasedClient.init();
			return ipPortBasedClient;
		});
		return true;
	}

	@Override
	public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
		return clientConnected(clientFactory.newSyncedClient(clientId, attributes));
	}

	@Override
	public boolean clientDisconnected(String clientId) {
		Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers", clientId);
		IpPortBasedClient client = clients.remove(clientId);
		if (null == client) {
			return true;
		}
		NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client));
		client.release();
		return true;
	}

	@Override
	public Client getClient(String clientId) {
		return clients.get(clientId);
	}

	@Override
	public boolean contains(String clientId) {
		return clients.containsKey(clientId);
	}

	@Override
	public Collection<String> allClientId() {
		return clients.keySet();
	}

	@Override
	public boolean isResponsibleClient(Client client) {
		if (client instanceof IpPortBasedClient) {
			return distroMapper.responsible(((IpPortBasedClient) client).getResponsibleId());
		}
		return false;
	}

	@Override
	public boolean verifyClient(String clientId) {
		// 从客户端管理器中获取客户端实例
		IpPortBasedClient client = clients.get(clientId);
		if (null != client) {
			// 若不为空，启动一个心跳更新任务
//        	每个节点在应用启动完毕之后延迟5秒，之后间隔5秒向其他节点发送Verify请求。
//        	每个节点只发送自己负责的、且未被移除的Client，且每一个Client都会发送一次请求，请求参数里面只附带了Client的clientId属性。(意味着当前节点只会告诉其他节点我目前有这个Client正在提供服务，并未提供服务的具体信息。)
//        	接收Verify请求的节点从请求参数中获取clientId，并检查自身是否有这个Client，若此Client存在，则更新Client下的所有Instance、以及Client自身的最新活跃时间为当前时间。

			NamingExecuteTaskDispatcher.getInstance().dispatchAndExecuteTask(clientId,
					new ClientBeatUpdateTask(client));
			return true;
		}
		return false;
	}

	private static class ExpiredClientCleaner implements Runnable {

		private final EphemeralIpPortClientManager clientManager;

		private final SwitchDomain switchDomain;

		public ExpiredClientCleaner(EphemeralIpPortClientManager clientManager, SwitchDomain switchDomain) {
			this.clientManager = clientManager;
			this.switchDomain = switchDomain;
		}

		@Override
		public void run() {
			long currentTime = System.currentTimeMillis();
			for (String each : clientManager.allClientId()) {
				IpPortBasedClient client = (IpPortBasedClient) clientManager.getClient(each);
				if (null != client && isExpireClient(currentTime, client)) {
					clientManager.clientDisconnected(each);
				}
			}
		}

		private boolean isExpireClient(long currentTime, IpPortBasedClient client) {
			long noUpdatedTime = currentTime - client.getLastUpdatedTime();
			return client.isEphemeral() && (isExpirePublishedClient(noUpdatedTime, client)
					&& isExpireSubscriberClient(noUpdatedTime, client)
					|| noUpdatedTime > ClientConfig.getInstance().getClientExpiredTime());
		}

		private boolean isExpirePublishedClient(long noUpdatedTime, IpPortBasedClient client) {
			return client.getAllPublishedService().isEmpty() && noUpdatedTime > Constants.DEFAULT_IP_DELETE_TIMEOUT;
		}

		private boolean isExpireSubscriberClient(long noUpdatedTime, IpPortBasedClient client) {
			return client.getAllSubscribeService().isEmpty()
					|| noUpdatedTime > switchDomain.getDefaultPushCacheMillis();
		}
	}
}
