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

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.utils.ReflectUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.utils.Loggers;
import io.grpc.Attributes;
import io.grpc.CompressorRegistry;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.DecompressorRegistry;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.internal.ServerStream;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Grpc implementation as a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
/**
 * gRPC服务器基类，实现RPC服务器的核心功能： 1. 初始化gRPC服务器并启动 2. 注册一元调用和双向流服务 3. 管理客户端连接的生命周期 4.
 * 传递连接元数据（IP、端口、连接ID）到业务上下文
 *
 * @author liuzunfei 开发者信息
 * @version 版本号及日期
 */
public abstract class BaseGrpcServer extends BaseRpcServer {

	private Server server; // gRPC服务器实例（核心网络处理组件）

	// --------------------- 常量定义（服务名、方法名、配置属性）---------------------
	private static final String REQUEST_BI_STREAM_SERVICE_NAME = "BiRequestStream"; // 双向流服务名称
	private static final String REQUEST_BI_STREAM_METHOD_NAME = "requestBiStream"; // 双向流方法名称
	private static final String REQUEST_SERVICE_NAME = "Request"; // 一元调用服务名称
	private static final String REQUEST_METHOD_NAME = "request"; // 一元调用方法名称
	private static final String GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY = "nacos.remote.server.grpc.maxinbound.message.size"; // 最大入站消息大小配置项
	private static final long DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE = 10 * 1024 * 1024; // 默认10MB（防止大消息导致内存问题）

	// --------------------- Spring依赖注入（业务处理器和连接管理器）---------------------
	@Autowired
	private GrpcRequestAcceptor grpcCommonRequestAcceptor; // 一元调用请求处理器（业务逻辑）
	@Autowired
	private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor; // 双向流请求处理器（业务逻辑）
	@Autowired
	private ConnectionManager connectionManager; // 连接管理器（维护客户端连接列表）

	// --------------------- 重写父类方法：声明连接类型为gRPC ---------------------
	@Override
	public ConnectionType getConnectionType() {
		return ConnectionType.GRPC; // 返回gRPC连接类型，标识当前服务器使用的协议
	}

	// --------------------- 核心方法：启动gRPC服务器 ---------------------
	@Override
	public void startServer() throws Exception {
		MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry(); // 可变服务注册表（动态添加服务）

		// 定义服务器拦截器：将连接元数据（ID、IP、端口）绑定到gRPC上下文
		ServerInterceptor serverInterceptor = new ServerInterceptor() {
			@Override
			public <T, S> ServerCall.Listener<T> interceptCall(ServerCall<T, S> call, Metadata headers,
					ServerCallHandler<T, S> next) {
				// 从连接属性中获取元数据，构建包含连接信息的上下文
				Context ctx = Context.current()
						.withValue(CONTEXT_KEY_CONN_ID, call.getAttributes().get(TRANS_KEY_CONN_ID))
						.withValue(CONTEXT_KEY_CONN_REMOTE_IP, call.getAttributes().get(TRANS_KEY_REMOTE_IP))
						.withValue(CONTEXT_KEY_CONN_REMOTE_PORT, call.getAttributes().get(TRANS_KEY_REMOTE_PORT))
						.withValue(CONTEXT_KEY_CONN_LOCAL_PORT, call.getAttributes().get(TRANS_KEY_LOCAL_PORT));
				// 若为双向流服务，额外绑定底层Netty通道（用于流操作）
				if (REQUEST_BI_STREAM_SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
					ctx = ctx.withValue(CONTEXT_KEY_CHANNEL, getInternalChannel(call)); // 反射获取底层通道并绑定
				}
				return Contexts.interceptCall(ctx, call, headers, next); // 使用新上下文继续处理请求
			}
		};

		addServices(handlerRegistry, serverInterceptor); // 注册服务（一元调用+双向流）到注册表

		// 构建gRPC服务器：配置端口、线程池、消息大小、传输过滤器等
		server = ServerBuilder.forPort(getServicePort()) // 端口由子类实现
				.executor(getRpcExecutor()) // 线程池由子类实现（处理请求的执行器）
				.maxInboundMessageSize(getInboundMessageSize()) // 限制最大入站消息大小
				.fallbackHandlerRegistry(handlerRegistry) // 设置后备服务注册表（处理未注册方法）
				.addTransportFilter(new ServerTransportFilter() { // 连接生命周期过滤器
					@Override
					public Attributes transportReady(Attributes attrs) {
						// 解析远程/本地地址，生成唯一连接ID并记录日志
						InetSocketAddress remoteAddr = (InetSocketAddress) attrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
						String remoteIp = remoteAddr.getAddress().getHostAddress();
						int remotePort = remoteAddr.getPort();
						String connectionId = System.currentTimeMillis() + "_" + remoteIp + "_" + remotePort;
						// 向连接属性中添加自定义元数据（连接ID、IP、端口）
						Attributes newAttrs = attrs.toBuilder().set(TRANS_KEY_CONN_ID, connectionId)
								.set(TRANS_KEY_REMOTE_IP, remoteIp).set(TRANS_KEY_REMOTE_PORT, remotePort)
								.set(TRANS_KEY_LOCAL_PORT,
										((InetSocketAddress) attrs.get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR)).getPort())
								.build();
						Loggers.REMOTE_DIGEST.info("Connection ready: {}", connectionId); // 记录连接建立日志
						return newAttrs;
					}

					@Override
					public void transportTerminated(Attributes attrs) {
						// 连接断开时，从连接管理器中注销连接
						String connectionId = attrs.get(TRANS_KEY_CONN_ID);
						if (StringUtils.isNotBlank(connectionId)) {
							Loggers.REMOTE_DIGEST.info("Connection terminated: {}", connectionId);
							connectionManager.unregister(connectionId); // 释放连接资源
						}
					}
				}).build();

		server.start(); // 启动服务器（异步启动，不阻塞当前线程）
	}

	// 获取最大入站消息大小（支持通过系统属性配置，默认10MB）
	private int getInboundMessageSize() {
		return Integer.parseInt(System.getProperty(GRPC_MAX_INBOUND_MSG_SIZE_PROPERTY,
				String.valueOf(DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE)));
	}

	// 反射获取gRPC调用的底层Netty通道（仅双向流服务需要）
	private Channel getInternalChannel(ServerCall serverCall) {
		ServerStream serverStream = (ServerStream) ReflectUtils.getFieldValue(serverCall, "stream"); // 反射获取内部流对象
		return (Channel) ReflectUtils.getFieldValue(serverStream, "channel"); // 反射获取底层通道
	}

	// --------------------- 服务注册方法：添加一元调用和双向流服务 ---------------------
	private void addServices(MutableHandlerRegistry registry, ServerInterceptor... interceptors) {
		// --------------------- 注册一元调用服务（单请求-单响应）---------------------
		MethodDescriptor<Payload, Payload> unaryMethod = MethodDescriptor.<Payload, Payload>newBuilder()
				.setType(MethodDescriptor.MethodType.UNARY) // 一元调用类型
				.setFullMethodName(MethodDescriptor.generateFullMethodName(REQUEST_SERVICE_NAME, REQUEST_METHOD_NAME))
				.setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())) // Protobuf序列化器
				.setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
		// --一问一回答
		ServerCallHandler<Payload, Payload> unaryHandler = ServerCalls.asyncUnaryCall(
				(request, responseObserver) -> grpcCommonRequestAcceptor.request(request, responseObserver)); // 委托业务处理器处理请求
		// 一元通信处理类
		ServerServiceDefinition unaryService = ServerServiceDefinition.builder(REQUEST_SERVICE_NAME)
				.addMethod(unaryMethod, unaryHandler).build();
		registry.addService(ServerInterceptors.intercept(unaryService, interceptors)); // 添加服务并应用拦截器

		// --------------------- 注册双向流服务（双向数据流）---------------------
		MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
				.setType(MethodDescriptor.MethodType.BIDI_STREAMING) // 双向流类型
				.setFullMethodName(MethodDescriptor.generateFullMethodName(REQUEST_BI_STREAM_SERVICE_NAME,
						REQUEST_BI_STREAM_METHOD_NAME))
				.setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
				.setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
		ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
				responseObserver -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver)); // 双向流处理器

		ServerServiceDefinition biStreamService = ServerServiceDefinition.builder(REQUEST_BI_STREAM_SERVICE_NAME)
				.addMethod(biStreamMethod, biStreamHandler).build();
		registry.addService(ServerInterceptors.intercept(biStreamService, interceptors)); // 添加服务并应用拦截器
	}

	// --------------------- 关闭服务器：释放资源 ---------------------
	@Override
	public void shutdownServer() {
		if (server != null) {
			server.shutdownNow(); // 立即关闭服务器，停止处理所有请求
		}
	}

	// --------------------- 抽象方法：子类需实现线程池和端口 ---------------------
	public abstract ThreadPoolExecutor getRpcExecutor(); // 获取处理请求的线程池（子类实现）

	// --------------------- 连接元数据键（Attributes和Context使用）---------------------
	// Attributes键：存储在gRPC连接属性中（跨调用链传递元数据）
	static final Attributes.Key<String> TRANS_KEY_CONN_ID = Attributes.Key.create("conn_id"); // 连接ID
	static final Attributes.Key<String> TRANS_KEY_REMOTE_IP = Attributes.Key.create("remote_ip"); // 远程IP
	static final Attributes.Key<Integer> TRANS_KEY_REMOTE_PORT = Attributes.Key.create("remote_port"); // 远程端口
	static final Attributes.Key<Integer> TRANS_KEY_LOCAL_PORT = Attributes.Key.create("local_port"); // 本地端口

	// Context键：存储在gRPC上下文中（跨服务方法传递元数据）
	static final Context.Key<String> CONTEXT_KEY_CONN_ID = Context.key("conn_id"); // 连接ID上下文
	static final Context.Key<String> CONTEXT_KEY_CONN_REMOTE_IP = Context.key("remote_ip"); // 远程IP上下文
	static final Context.Key<Integer> CONTEXT_KEY_CONN_REMOTE_PORT = Context.key("remote_port"); // 远程端口上下文
	static final Context.Key<Integer> CONTEXT_KEY_CONN_LOCAL_PORT = Context.key("local_port"); // 本地端口上下文
	static final Context.Key<Channel> CONTEXT_KEY_CHANNEL = Context.key("ctx_channel"); // Netty通道上下文（双向流专用）
}