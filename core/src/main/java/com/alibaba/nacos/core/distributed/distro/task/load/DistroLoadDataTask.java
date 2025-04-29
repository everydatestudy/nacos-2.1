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

package com.alibaba.nacos.core.distributed.distro.task.load;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * https://www.cnblogs.com/lukama/p/14918667.html Distro load data task.
 *
 * @author xiweng.yy
 */
public class DistroLoadDataTask implements Runnable {
	// 节点管理器
	private final ServerMemberManager memberManager;
	// Distro协议组件持有者
	private final DistroComponentHolder distroComponentHolder;
	// Distro协议配置
	private final DistroConfig distroConfig;
	// 回调函数
	private final DistroCallback loadCallback;
	// 已加载数据集合
	private final Map<String, Boolean> loadCompletedMap;

	public DistroLoadDataTask(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
			DistroConfig distroConfig, DistroCallback loadCallback) {
		this.memberManager = memberManager;
		this.distroComponentHolder = distroComponentHolder;
		this.distroConfig = distroConfig;
		this.loadCallback = loadCallback;
		loadCompletedMap = new HashMap<>(1);
	}

	@Override
	public void run() {
		try {
			// 首次加载
			load();
			// 若首次加载没有完成，继续加载
			if (!checkCompleted()) {
				// 继续创建一个新的加载任务进行加载
				GlobalExecutor.submitLoadDataTask(this, distroConfig.getLoadDataRetryDelayMillis());
			} else {
				// 触发回调函数
				loadCallback.onSuccess();
				Loggers.DISTRO.info("[DISTRO-INIT] load snapshot data success");
			}
		} catch (Exception e) {
			loadCallback.onFailed(e);
			Loggers.DISTRO.error("[DISTRO-INIT] load snapshot data failed. ", e);
		}
	}

	private void load() throws Exception {
		// 若出自身之外没有其他节点，则休眠1秒，可能其他节点还未启动完毕

		while (memberManager.allMembersWithoutSelf().isEmpty()) {
			Loggers.DISTRO.info("[DISTRO-INIT] waiting server list init...");
			TimeUnit.SECONDS.sleep(1);
		}
		// 若数据类型为空，说明distroComponentHolder的组件注册器还未初始化完毕（v1版本为DistroHttpRegistry,
		// v2版本为DistroClientComponentRegistry）
		while (distroComponentHolder.getDataStorageTypes().isEmpty()) {
			Loggers.DISTRO.info("[DISTRO-INIT] waiting distro data storage register...");
			TimeUnit.SECONDS.sleep(1);
		}
		// 加载每个类型的数据
		for (String each : distroComponentHolder.getDataStorageTypes()) {
			if (!loadCompletedMap.containsKey(each) || !loadCompletedMap.get(each)) {
				// 调用加载方法，并标记已处理
				// 处理所有协议类型，这里只有V2 Grpc类型
				loadCompletedMap.put(each, loadAllDataSnapshotFromRemote(each));
			}
		}
	}

	/**
	 * 从其他节点获取同步数据
	 * 
	 * @param resourceType
	 * @return
	 */
	private boolean loadAllDataSnapshotFromRemote(String resourceType) {
		// 获取数据传输对象
		DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
		// 获取数据处理器
		DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
		if (null == transportAgent || null == dataProcessor) {
			Loggers.DISTRO.warn("[DISTRO-INIT] Can't find component for type {}, transportAgent: {}, dataProcessor: {}",
					resourceType, transportAgent, dataProcessor);
			return false;
		}
		// 向每个节点请求数据
		for (Member each : memberManager.allMembersWithoutSelf()) {
			try {
				Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {}", resourceType, each.getAddress());
				// 获取到数据
				DistroData distroData = transportAgent.getDatumSnapshot(each.getAddress());
				// 解析数据
				boolean result = dataProcessor.processSnapshot(distroData);
				Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {} result: {}", resourceType,
						each.getAddress(), result);
				// 若解析成功，标记此类型数据已加载完毕
				if (result) {
					distroComponentHolder.findDataStorage(resourceType).finishInitial();
					return true;
				}
			} catch (Exception e) {
				Loggers.DISTRO.error("[DISTRO-INIT] load snapshot {} from {} failed.", resourceType, each.getAddress(),
						e);
			}
		}
		return false;
	}

	// 判断是否完成加载
	private boolean checkCompleted() {
		// 若待加载的数据类型数量和已经加载完毕的数据类型数量不一致，铁定是未加载完成
		if (distroComponentHolder.getDataStorageTypes().size() != loadCompletedMap.size()) {
			return false;
		}
		// 若加载完毕列表内的状态有false的，说明可能是解析失败，还需要重新加载
		for (Boolean each : loadCompletedMap.values()) {
			if (!each) {
				return false;
			}
		}
		return true;
	}
}
