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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroExecuteTaskExecuteEngine;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Timed to start distro verify task.
 *
 * @author xiweng.yy
 */
//定时验证任务，此任务在启动时延迟5秒，间隔5秒执行。主要用于为每个节点创建一个数据验证的执行任务DistroVerifyExecuteTask。它的数据处理维度是Member。
//启动Distro协议的数据验证流程
//验证流程中的任务产生说明：
//
//当前节点的DistroVerifyTimedTask 会根据节点的数量来创建DistroVerifyExecuteTask，
//并向其传递自身负责的所有Client的clientId集合（clientId最终被包装成DistroData）。
//每一个DistroVerifyExecuteTask 会为传入的List中的每一个DistroData创建一个异步的rpc请求。
public class DistroVerifyTimedTask implements Runnable {

	private final ServerMemberManager serverMemberManager;

	private final DistroComponentHolder distroComponentHolder;

	private final DistroExecuteTaskExecuteEngine executeTaskExecuteEngine;

	public DistroVerifyTimedTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder,
			DistroExecuteTaskExecuteEngine executeTaskExecuteEngine) {
		this.serverMemberManager = serverMemberManager;
		this.distroComponentHolder = distroComponentHolder;
		this.executeTaskExecuteEngine = executeTaskExecuteEngine;
	}

	@Override
	public void run() {
		try {
			// 获取除自身节点之外的其他节点
			List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.debug("server list is: {}", targetServer);
			}
			// 每一种类型的数据，都要向其他节点发起验证
			for (String each : distroComponentHolder.getDataStorageTypes()) {
				// 根据类型来验证，这个类型代表着协议类型，2.2.0的版本只会用Grpc的类型
				verifyForDataStorage(each, targetServer);
			}
		} catch (Exception e) {
			Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
		}
	}
//	在验证的处理方法中，先需要拿到非本机的其他节点，然后进行验证
//
//	验证的时候会首先拿到本机的数据，再对所有节点都执行验证操作
	private void verifyForDataStorage(String type, List<Member> targetServer) {
		// 获取数据类型
		DistroDataStorage dataStorage = distroComponentHolder.findDataStorage(type);
		// 若数据还未同步完毕则不处理
		if (!dataStorage.isFinishInitial()) {
			Loggers.DISTRO.warn("data storage {} has not finished initial step, do not send verify data",
					dataStorage.getClass().getSimpleName());
			return;
		}
		// ① 获取验证数据
		List<DistroData> verifyData = dataStorage.getVerifyData();
		if (null == verifyData || verifyData.isEmpty()) {
			return;
		}
		// 对每个节点开启一个异步的线程来执行
		for (Member member : targetServer) {
			DistroTransportAgent agent = distroComponentHolder.findTransportAgent(type);
			if (null == agent) {
				continue;
			}
			executeTaskExecuteEngine.addTask(member.getAddress() + type,
					new DistroVerifyExecuteTask(agent, verifyData, member.getAddress(), type));
		}
	}
}
