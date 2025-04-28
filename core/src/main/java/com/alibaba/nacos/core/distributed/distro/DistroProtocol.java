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

package com.alibaba.nacos.core.distributed.distro;

import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTask;
import com.alibaba.nacos.core.distributed.distro.task.load.DistroLoadDataTask;
import com.alibaba.nacos.core.distributed.distro.task.verify.DistroVerifyTimedTask;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

/**
 * Distro protocol.
 *
 * @author xiweng.yy
 */
@Component
public class DistroProtocol {

	/**
	 * 节点管理器
	 */
	private final ServerMemberManager memberManager;

	/**
	 * Distro组件持有者
	 */
	private final DistroComponentHolder distroComponentHolder;

	/**
	 * Distro任务引擎持有者
	 */
	private final DistroTaskEngineHolder distroTaskEngineHolder;

	private volatile boolean isInitialized = false;

	public DistroProtocol(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
			DistroTaskEngineHolder distroTaskEngineHolder) {
		this.memberManager = memberManager;
		this.distroComponentHolder = distroComponentHolder;
		this.distroTaskEngineHolder = distroTaskEngineHolder;
		// 启动Distro协议
		startDistroTask();
	}

	private void startDistroTask() {
		if (EnvUtil.getStandaloneMode()) {
			isInitialized = true;
			return;
		}
		// 开启节点Client状态报告任务
		startVerifyTask();
		// 启动数据同步任务
		startLoadTask();
	}

	/**
	 * 从其他节点获取数据到当前节点
	 */
	private void startLoadTask() {
		DistroCallback loadCallback = new DistroCallback() {
			@Override
			public void onSuccess() {
				isInitialized = true;
			}

			@Override
			public void onFailed(Throwable throwable) {
				isInitialized = false;
			}
		};
		// 验证功能从startVerifyTask()方法开始启动，此处它构建了一个名为DistroVerifyTimedTask的定时任务，延迟5秒开始，间隔5秒轮询。
		GlobalExecutor.submitLoadDataTask(
				new DistroLoadDataTask(memberManager, distroComponentHolder, DistroConfig.getInstance(), loadCallback));
	}

	private void startVerifyTask() {
		GlobalExecutor.schedulePartitionDataTimedSync(
				new DistroVerifyTimedTask(memberManager, distroComponentHolder,
						distroTaskEngineHolder.getExecuteWorkersManager()),
				DistroConfig.getInstance().getVerifyIntervalMillis());
	}

	public boolean isInitialized() {
		return isInitialized;
	}

	/**
	 * Start to sync by configured delay.
	 *
	 * @param distroKey distro key of sync data
	 * @param action    the action of data operation
	 */
	// 按配置的延迟开始同步
	public void sync(DistroKey distroKey, DataOperation action) {
		// 配置同步延迟的时间：默认为1s
		sync(distroKey, action, DistroConfig.getInstance().getSyncDelayMillis());
	}

	/**
	 * Start to sync data to all remote server.
	 *
	 * @param distroKey distro key of sync data
	 * @param action    the action of data operation
	 * @param delay     delay time for sync
	 */
	// 开始将数据同步到其他节点
	public void sync(DistroKey distroKey, DataOperation action, long delay) {
		// 拿到集群中除了自己的其他节点
		for (Member each : memberManager.allMembersWithoutSelf()) {
			syncToTarget(distroKey, action, each.getAddress(), delay);
		}
	}

	/**
	 * Start to sync to target server.
	 *
	 * @param distroKey    distro key of sync data
	 * @param action       the action of data operation
	 * @param targetServer target server
	 * @param delay        delay time for sync
	 */
	public void syncToTarget(DistroKey distroKey, DataOperation action, String targetServer, long delay) {
		DistroKey distroKeyWithTarget = new DistroKey(distroKey.getResourceKey(), distroKey.getResourceType(),
				targetServer);
		// 构建一个延迟任务
		DistroDelayTask distroDelayTask = new DistroDelayTask(distroKeyWithTarget, action, delay);
		// 往延时任务引擎中加入DistroDelayTask任务,最终将会调用DistroDelayTaskProcessor.process方法
		// distroTaskEngineHolder.getDelayTaskExecuteEngine()返回的是DistroDelayTaskExecuteEngine，它继承自NacosDelayTaskExecuteEngine,
		// 其构造方法中初始化了一个定时任务ProcessRunnable，不断从阻塞队列中拿出任务出来执行（ConcurrentHashMap<Object,
		// AbstractDelayTask> tasks）
		distroTaskEngineHolder.getDelayTaskExecuteEngine().addTask(distroKeyWithTarget, distroDelayTask);
		if (Loggers.DISTRO.isDebugEnabled()) {
			Loggers.DISTRO.debug("[DISTRO-SCHEDULE] {} to {}", distroKey, targetServer);
		}
	}

	/**
	 * Query data from specified server.
	 *
	 * @param distroKey data key
	 * @return data
	 */
	/**
	 * Query data from specified server. 从指定的节点获取DistroData
	 * 
	 * @param distroKey data key
	 * @return data
	 */
	public DistroData queryFromRemote(DistroKey distroKey) {
		if (null == distroKey.getTargetServer()) {
			Loggers.DISTRO.warn("[DISTRO] Can't query data from empty server");
			return null;
		}
		String resourceType = distroKey.getResourceType();
		DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
		if (null == transportAgent) {
			Loggers.DISTRO.warn("[DISTRO] Can't find transport agent for key {}", resourceType);
			return null;
		}// 使用DistroTransportAgent获取数据
		return transportAgent.getData(distroKey, distroKey.getTargetServer());
	}

	/**
	 * Receive synced distro data, find processor to process.
	 *
	 * @param distroData Received data
	 * @return true if handle receive data successfully, otherwise false
	 */
	// 接收到同步数据，并查找处理器进行处理
	public boolean onReceive(DistroData distroData) {
		Loggers.DISTRO.info("[DISTRO] Receive distro data type: {}, key: {}", distroData.getType(),
				distroData.getDistroKey());
		String resourceType = distroData.getDistroKey().getResourceType();
		DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
		if (null == dataProcessor) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data process for received data {}", resourceType);
			return false;
		}
		return dataProcessor.processData(distroData);
	}

	/**
	 * Receive verify data, find processor to process.
	 *
	 * @param distroData    verify data
	 * @param sourceAddress source server address, might be get data from source
	 *                      server
	 * @return true if verify data successfully, otherwise false
	 */
	// 根据不同类型获取不同的数据处理器
	public boolean onVerify(DistroData distroData, String sourceAddress) {
		if (Loggers.DISTRO.isDebugEnabled()) {
			Loggers.DISTRO.debug("[DISTRO] Receive verify data type: {}, key: {}", distroData.getType(),
					distroData.getDistroKey());
		}
		// 根据此次处理的数据类型获取对应的处理器，此处我们处理的类型是Client类型（Nacos:Naming:v2:ClientData）
		String resourceType = distroData.getDistroKey().getResourceType();
		DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
		if (null == dataProcessor) {
			Loggers.DISTRO.warn("[DISTRO] Can't find verify data process for received data {}", resourceType);
			return false;
		}
		return dataProcessor.processVerifyData(distroData, sourceAddress);
	}

	/**
	 * Query data of input distro key.
	 *
	 * @param distroKey key of data
	 * @return data
	 */
	public DistroData onQuery(DistroKey distroKey) {
		String resourceType = distroKey.getResourceType();
		DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(resourceType);
		if (null == distroDataStorage) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", resourceType);
			return new DistroData(distroKey, new byte[0]);
		}
		return distroDataStorage.getDistroData(distroKey);
	}

	/**
	 * Query all datum snapshot.
	 *
	 * @param type datum type
	 * @return all datum snapshot
	 */
	// 查询所有快照数据
	public DistroData onSnapshot(String type) {
		DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(type);
		if (null == distroDataStorage) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", type);
			return new DistroData(new DistroKey("snapshot", type), new byte[0]);
		}
		return distroDataStorage.getDatumSnapshot();
	}
}
