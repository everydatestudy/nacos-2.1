/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.monitor.DistroRecord;
import com.alibaba.nacos.core.distributed.distro.monitor.DistroRecordsHolder;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Execute distro verify task.
 *
 * @author xiweng.yy
 */
//Distro数据验证任务执行器，用于向其他节点发送当前节点负责的Client状态报告，通知对方此Client正常服务。它的数据处理维度是DistroData。
//执行Distro协议数据验证的任务，为每个DistroData发送一个异步的rpc请求
public class DistroVerifyExecuteTask extends AbstractExecuteTask {
	/**
	 * 被验证数据的传输对象
	 */
	private final DistroTransportAgent transportAgent;
	/**
	 * 被验证数据
	 */
	private final List<DistroData> verifyData;
	/**
	 * 目标节点
	 */
	private final String targetServer;
	/**
	 * 被验证数据的类型
	 */
	private final String resourceType;

	public DistroVerifyExecuteTask(DistroTransportAgent transportAgent, List<DistroData> verifyData,
			String targetServer, String resourceType) {
		this.transportAgent = transportAgent;
		this.verifyData = verifyData;
		this.targetServer = targetServer;
		this.resourceType = resourceType;
	}

	@Override
	public void run() {
		for (DistroData each : verifyData) {
			try { // 判断传输对象是否支持回调（若是http的则不支持，实际上没区别，当前2.0.1版本没有实现回调的实质内容）
				if (transportAgent.supportCallbackTransport()) {
					doSyncVerifyDataWithCallback(each);
				} else {
					doSyncVerifyData(each);
				}
			} catch (Exception e) {
				Loggers.DISTRO.error("[DISTRO-FAILED] verify data for type {} to {} failed.", resourceType,
						targetServer, e);
			}
		}
	}

	/**
	 * 支持回调的同步数据验证
	 * 
	 * @param data
	 */
	private void doSyncVerifyDataWithCallback(DistroData data) {
		// 回调实际上，也没啥。。。基本算是空对象
		transportAgent.syncVerifyData(data, targetServer, new DistroVerifyCallback());
	}

	/**
	 * 不支持回调的同步数据验证
	 * 
	 * @param data
	 */
	private void doSyncVerifyData(DistroData data) {
		transportAgent.syncVerifyData(data, targetServer);
	}

	private class DistroVerifyCallback implements DistroCallback {

		@Override
		public void onSuccess() {
			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.debug("[DISTRO] verify data for type {} to {} success", resourceType, targetServer);
			}
		}

		@Override
		public void onFailed(Throwable throwable) {
			DistroRecord distroRecord = DistroRecordsHolder.getInstance().getRecord(resourceType);
			distroRecord.verifyFail();
			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.debug("[DISTRO-FAILED] verify data for type {} to {} failed.", resourceType,
						targetServer, throwable);
			}
		}
	}
}
