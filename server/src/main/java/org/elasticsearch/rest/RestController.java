/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.usage.UsageService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.METHOD_NOT_ALLOWED;
import static org.elasticsearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * 处理http请求的控制器
 */
public class RestController implements HttpServerTransport.Dispatcher {

    private static final Logger logger = LogManager.getLogger(RestController.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    /**
     * 通过trie的算法 实现路径/handler 匹配
     * 针对同一路径的多个method 会通过同一个methodHandler处理
     * 在methodHandler内部 通过method 来匹配不同的restHandler
     * restHandler 就是最小粒度的处理器
     */
    private final PathTrie<MethodHandlers> handlers = new PathTrie<>(RestUtils.REST_DECODER);

    private final UnaryOperator<RestHandler> handlerWrapper;

    private final NodeClient client;

    private final CircuitBreakerService circuitBreakerService;

    /**
     * Rest headers that are copied to internal requests made during a rest request.
     * 这里定义了请求头的规则   每个收到的req对象需要通过这组规则进行校验  匹配成功的请求头将会存储一份到ThreadContext上
     * 便于在后面的处理中随时获取
     * RestHeaderDefinition.name 代表什么请求头会受到约束
     * RestHeaderDefinition.multiValueAllowed  说明这个请求头是否支持多个value
     */
    private final Set<RestHeaderDefinition> headersToCopy;
    private final UsageService usageService;

    public RestController(Set<RestHeaderDefinition> headersToCopy, UnaryOperator<RestHandler> handlerWrapper,
            NodeClient client, CircuitBreakerService circuitBreakerService, UsageService usageService) {
        this.headersToCopy = headersToCopy;
        this.usageService = usageService;
        if (handlerWrapper == null) {
            handlerWrapper = h -> h; // passthrough if no wrapper set
        }
        this.handlerWrapper = handlerWrapper;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param deprecationMessage The message to log and send as a header in the response
     */
    protected void registerAsDeprecatedHandler(RestRequest.Method method, String path, RestHandler handler, String deprecationMessage) {
        assert (handler instanceof DeprecationRestHandler) == false;

        registerHandler(method, path, new DeprecationRestHandler(handler, deprecationMessage, deprecationLogger));
    }

    /**
     * Registers a REST handler to be executed when the provided {@code method} and {@code path} match the request, or when provided
     * with {@code deprecatedMethod} and {@code deprecatedPath}. Expected usage:
     * <pre><code>
     * // remove deprecation in next major release
     * controller.registerWithDeprecatedHandler(POST, "/_forcemerge", this,
     *                                          POST, "/_optimize", deprecationLogger);
     * controller.registerWithDeprecatedHandler(POST, "/{index}/_forcemerge", this,
     *                                          POST, "/{index}/_optimize", deprecationLogger);
     * </code></pre>
     * <p>
     * The registered REST handler ({@code method} with {@code path}) is a normal REST handler that is not deprecated and it is
     * replacing the deprecated REST handler ({@code deprecatedMethod} with {@code deprecatedPath}) that is using the <em>same</em>
     * {@code handler}.
     * <p>
     * Deprecated REST handlers without a direct replacement should be deprecated directly using {@link #registerAsDeprecatedHandler}
     * and a specific message.
     *
     * @param method GET, POST, etc.
     * @param path Path to handle (e.g., "/_forcemerge")
     * @param handler The handler to actually execute
     * @param deprecatedMethod GET, POST, etc.
     * @param deprecatedPath <em>Deprecated</em> path to handle (e.g., "/_optimize")
     */
    protected void registerWithDeprecatedHandler(RestRequest.Method method, String path, RestHandler handler,
                                              RestRequest.Method deprecatedMethod, String deprecatedPath) {
        // e.g., [POST /_optimize] is deprecated! Use [POST /_forcemerge] instead.
        final String deprecationMessage =
            "[" + deprecatedMethod.name() + " " + deprecatedPath + "] is deprecated! Use [" + method.name() + " " + path + "] instead.";

        registerHandler(method, path, handler);
        registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage);
    }

    /**
     * Registers a REST handler to be executed when one of the provided methods and path match the request.
     *
     * @param path Path to handle (e.g., "/{index}/{type}/_bulk")
     * @param handler The handler to actually execute
     * @param method GET, POST, etc.
     *               将处理器注册到PathTrie中
     */
    protected void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        if (handler instanceof BaseRestHandler) {
            usageService.addRestHandler((BaseRestHandler) handler);
        }
        final RestHandler maybeWrappedHandler = handlerWrapper.apply(handler);
        handlers.insertOrUpdate(path, new MethodHandlers(path, maybeWrappedHandler, method),
            (mHandlers, newMHandler) -> mHandlers.addMethods(maybeWrappedHandler, method));
    }

    /**
     * Registers a REST handler with the controller. The REST handler declares the {@code method}
     * and {@code path} combinations.
     *
     */
    public void registerHandler(final RestHandler restHandler) {
        // routes 返回这个handler允许处理的所有 route   route包含path/method2个关键属性
        restHandler.routes().forEach(route -> registerHandler(route.getMethod(), route.getPath(), restHandler));
        // TODO 忽略兼容性代码
        restHandler.deprecatedRoutes().forEach(route ->
            registerAsDeprecatedHandler(route.getMethod(), route.getPath(), restHandler, route.getDeprecationMessage()));
        restHandler.replacedRoutes().forEach(route -> registerWithDeprecatedHandler(route.getMethod(), route.getPath(),
            restHandler, route.getDeprecatedMethod(), route.getDeprecatedPath()));
    }

    /**
     * 当接收到一个rest请求后 分发到对应handler进行处理
     * @param request       the request to dispatch
     * @param channel       the response channel of this request  该通道是用于返回结果的
     * @param threadContext the thread context
     */
    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        // TODO
        if (request.rawPath().equals("/favicon.ico")) {
            handleFavicon(request.method(), request.uri(), channel);
            return;
        }
        try {
            tryAllHandlers(request, channel, threadContext);
        } catch (Exception e) {
            try {
                // 将异常信息转换成数据流 并发送到对端
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() ->
                    new ParameterizedMessage("failed to send failure response for uri [{}]", request.uri()), inner);
            }
        }
    }

    /**
     * 当接收到一个畸形的req时 触发该方法
     */
    @Override
    public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
        try {
            final Exception e;
            if (cause == null) {
                e = new ElasticsearchException("unknown cause");
            } else if (cause instanceof Exception) {
                e = (Exception) cause;
            } else {
                e = new ElasticsearchException(cause);
            }
            // 直接将异常信息返回给对端
            channel.sendResponse(new BytesRestResponse(channel, BAD_REQUEST, e));
        } catch (final IOException e) {
            if (cause != null) {
                e.addSuppressed(cause);
            }
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    /**
     * 使用处理器 处理req对象
     * @param request
     * @param channel  通过该对象可以将数据流写回给客户端
     * @param handler
     * @throws Exception
     */
    private void dispatchRequest(RestRequest request, RestChannel channel, RestHandler handler) throws Exception {
        final int contentLength = request.contentLength();
        if (contentLength > 0) {
            // 请求体中的 XContentType 就是通过解析请求头携带的Content-Type获得的
            final XContentType xContentType = request.getXContentType();
            if (xContentType == null) {
                sendContentTypeErrorMessage(request.getAllHeaderValues("Content-Type"), channel);
                return;
            }
            // 当handler支持处理数据流    要求此时contentType 必须是json或者smile  啥意思 ???
            if (handler.supportsContentStream() && xContentType != XContentType.JSON && xContentType != XContentType.SMILE) {
                channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, RestStatus.NOT_ACCEPTABLE,
                    "Content-Type [" + xContentType + "] does not support stream parsing. Use JSON or SMILE instead"));
                return;
            }
        }
        RestChannel responseChannel = channel;
        try {
            // 这个处理器是否受熔断器限制
            if (handler.canTripCircuitBreaker()) {
                inFlightRequestsBreaker(circuitBreakerService).addEstimateBytesAndMaybeBreak(contentLength, "<http_request>");
            } else {
                // withoutBreaking 会增加负载量 但是不会触发熔断  这样其他action会更容易达到熔断值
                inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(contentLength);
            }
            // iff we could reserve bytes for the request we need to send the response also over this channel
            responseChannel = new ResourceHandlingHttpChannel(channel, circuitBreakerService, contentLength);
            // TODO: Count requests double in the circuit breaker if they need copying?
            // 这一步的含义是什么 特地拷贝了一份 httpRequest
            if (handler.allowsUnsafeBuffers() == false) {
                request.ensureSafeBuffers();
            }
            handler.handleRequest(request, responseChannel, client);
        } catch (Exception e) {
            // 当处理失败时 将异常信息序列化并发送给client
            responseChannel.sendResponse(new BytesRestResponse(responseChannel, e));
        }
    }

    /**
     * 当指定path/method找不到匹配的handler时
     * @param rawPath
     * @param method
     * @param uri
     * @param channel
     * @return
     */
    private boolean handleNoHandlerFound(String rawPath, RestRequest.Method method, String uri, RestChannel channel) {
        // Get the map of matching handlers for a request, for the full set of HTTP methods.
        final Set<RestRequest.Method> validMethodSet = getValidHandlerMethodSet(rawPath);
        // 如果这个method总集不包含 入参方法  就代表确实不存在对应的handler
        if (validMethodSet.contains(method) == false) {
            if (method == RestRequest.Method.OPTIONS) {
                // 如果是Options 会返回支持的所有method到请求端
                handleOptionsRequest(channel, validMethodSet);
                return true;
            }
            // 代表如果更换method就可以与handler匹配成功  需要将提示信息返回给对端
            if (validMethodSet.isEmpty() == false) {
                // If an alternative handler for an explicit path is registered to a
                // different HTTP method than the one supplied - return a 405 Method
                // Not Allowed error.
                handleUnsupportedHttpMethod(uri, method, channel, validMethodSet, null);
                return true;
            }
        }
        return false;
    }

    /**
     * 将错误信息返回给客户端 有关内容体格式的
     * @param contentTypeHeader
     * @param channel
     * @throws IOException
     */
    private void sendContentTypeErrorMessage(@Nullable List<String> contentTypeHeader, RestChannel channel) throws IOException {
        final String errorMessage;
        if (contentTypeHeader == null) {
            errorMessage = "Content-Type header is missing";
        } else {
            errorMessage = "Content-Type header [" +
                Strings.collectionToCommaDelimitedString(contentTypeHeader) + "] is not supported";
        }

        channel.sendResponse(BytesRestResponse.createSimpleErrorResponse(channel, NOT_ACCEPTABLE, errorMessage));
    }

    /**
     * 在处理正常的 RestReq时 会转发到该方法
     * @param request
     * @param channel
     * @param threadContext
     * @throws Exception
     */
    private void tryAllHandlers(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) throws Exception {
        // 这部分请求头会存储到 ThreadContext中
        for (final RestHeaderDefinition restHeader : headersToCopy) {
            final String name = restHeader.getName();
            // 请求头中一个name 可能会对应多个value的
            final List<String> headerValues = request.getAllHeaderValues(name);
            if (headerValues != null && headerValues.isEmpty() == false) {
                final List<String> distinctHeaderValues = headerValues.stream().distinct().collect(Collectors.toList());
                // 在请求头不允许多个value时 检测到多个value 直接将异常信息返回
                if (restHeader.isMultiValueAllowed() == false && distinctHeaderValues.size() > 1) {
                    channel.sendResponse(
                        BytesRestResponse.
                            createSimpleErrorResponse(channel, BAD_REQUEST, "multiple values for single-valued header [" + name + "]."));
                    return;
                } else {
                    // 将请求头信息设置到当前线程绑定的上下文中
                    // ThreadContext 在很多方法中都有使用 为了方便使用者获取此时绑定在线程上的特殊信息
                    threadContext.putHeader(name, String.join(",", distinctHeaderValues));
                }
            }
        }
        // error_trace cannot be used when we disable detailed errors
        // we consume the error_trace parameter first to ensure that it is always consumed
        // 当channel 不支持追踪异常信息 而在请求头中指定要追踪 则代表这是一个不合理的请求
        if (request.paramAsBoolean("error_trace", false) && channel.detailedErrorsEnabled() == false) {
            channel.sendResponse(
                    BytesRestResponse.createSimpleErrorResponse(channel, BAD_REQUEST, "error traces in responses are disabled."));
            return;
        }

        final String rawPath = request.rawPath();
        final String uri = request.uri();
        final RestRequest.Method requestMethod;
        try {
            // Resolves the HTTP method and fails if the method is invalid
            requestMethod = request.method();
            // Loop through all possible handlers, attempting to dispatch the request
            // 获取所有符合条件的处理器
            Iterator<MethodHandlers> allHandlers = getAllHandlers(request.params(), rawPath);
            while (allHandlers.hasNext()) {
                final RestHandler handler;
                final MethodHandlers handlers = allHandlers.next();
                if (handlers == null) {
                    handler = null;
                } else {
                    handler = handlers.getHandler(requestMethod);
                }
                if (handler == null) {
                    // 返回true 就是已经将提示信息返回给对端了
                    // 返回false 会继续遍历下一个 MethodHandlers
                  if (handleNoHandlerFound(rawPath, requestMethod, uri, channel)) {
                      return;
                  }
                } else {
                    // 当找到匹配的handler时触发该方法
                    dispatchRequest(request, channel, handler);
                    return;
                }
            }
        } catch (final IllegalArgumentException e) {
            handleUnsupportedHttpMethod(uri, null, channel, getValidHandlerMethodSet(rawPath), e);
            return;
        }
        // If request has not been handled, fallback to a bad request error.
        // 代表不存在匹配的handler 也没有handler的相关提示信息返回给请求端    代表这是一个BadRequest
        handleBadRequest(uri, requestMethod, channel);
    }

    /**
     * 获取所有path命中的处理器
     * @param requestParamsRef   在解析过程中如果匹配上了 通配符 就将token，value存储到map中 相当于是从path上解析参数
     * @param rawPath  未使用url解码的路径
     * @return
     */
    Iterator<MethodHandlers> getAllHandlers(@Nullable Map<String, String> requestParamsRef, String rawPath) {
        final Supplier<Map<String, String>> paramsSupplier;
        if (requestParamsRef == null) {
            paramsSupplier = () -> null;
        } else {
            // Between retrieving the correct path, we need to reset the parameters,
            // otherwise parameters are parsed out of the URI that aren't actually handled.
            final Map<String, String> originalParams = Map.copyOf(requestParamsRef);

            // 因为在retrieveAll中会进行多次匹配  每次匹配之间的参数应该相互隔离 所以每次都是获得requestParamsRef的副本
            paramsSupplier = () -> {
                // PathTrie modifies the request, so reset the params between each iteration
                // 每次调用该函数 就获取一份 requestParamsRef
                requestParamsRef.clear();
                requestParamsRef.putAll(originalParams);
                return requestParamsRef;
            };
        }
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return handlers.retrieveAll(rawPath, paramsSupplier);
    }

    /**
     * Handle requests to a valid REST endpoint using an unsupported HTTP
     * method. A 405 HTTP response code is returned, and the response 'Allow'
     * header includes a list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-10.4.6">HTTP/1.1 -
     * 10.4.6 - 405 Method Not Allowed</a>).
     */
    private void handleUnsupportedHttpMethod(String uri,
                                             @Nullable RestRequest.Method method,
                                             final RestChannel channel,
                                             final Set<RestRequest.Method> validMethodSet,
                                             @Nullable final IllegalArgumentException exception) {
        try {
            final StringBuilder msg = new StringBuilder();
            if (exception == null) {
                msg.append("Incorrect HTTP method for uri [").append(uri);
                msg.append("] and method [").append(method).append("]");
            } else {
                msg.append(exception.getMessage());
            }
            if (validMethodSet.isEmpty() == false) {
                msg.append(", allowed: ").append(validMethodSet);
            }
            BytesRestResponse bytesRestResponse = BytesRestResponse.createSimpleErrorResponse(channel, METHOD_NOT_ALLOWED, msg.toString());
            if (validMethodSet.isEmpty() == false) {
                bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
            }
            channel.sendResponse(bytesRestResponse);
        } catch (final IOException e) {
            logger.warn("failed to send bad request response", e);
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }

    /**
     * Handle HTTP OPTIONS requests to a valid REST endpoint. A 200 HTTP
     * response code is returned, and the response 'Allow' header includes a
     * list of valid HTTP methods for the endpoint (see
     * <a href="https://tools.ietf.org/html/rfc2616#section-9.2">HTTP/1.1 - 9.2
     * - Options</a>).
     * @param validMethodSet 支持的所有方法类型
     */
    private void handleOptionsRequest(RestChannel channel, Set<RestRequest.Method> validMethodSet) {
        // 如果传入的是Optional 代表本次是一个探测请求  会返回所有支持的方法
        BytesRestResponse bytesRestResponse = new BytesRestResponse(OK, TEXT_CONTENT_TYPE, BytesArray.EMPTY);
        // When we have an OPTIONS HTTP request and no valid handlers, simply send OK by default (with the Access Control Origin header
        // which gets automatically added).
        if (validMethodSet.isEmpty() == false) {
            bytesRestResponse.addHeader("Allow", Strings.collectionToDelimitedString(validMethodSet, ","));
        }
        // 将结果返回给对端
        channel.sendResponse(bytesRestResponse);
    }

    /**
     * Handle a requests with no candidate handlers (return a 400 Bad Request
     * error).
     */
    private void handleBadRequest(String uri, RestRequest.Method method, RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newErrorBuilder()) {
            builder.startObject();
            {
                builder.field("error", "no handler found for uri [" + uri + "] and method [" + method + "]");
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder));
        }
    }

    /**
     * Get the valid set of HTTP methods for a REST request.
     * 返回当前所有匹配的handler的method总集
     */
    private Set<RestRequest.Method> getValidHandlerMethodSet(String rawPath) {
        Set<RestRequest.Method> validMethods = new HashSet<>();
        Iterator<MethodHandlers> allHandlers = getAllHandlers(null, rawPath);
        while (allHandlers.hasNext()) {
            final MethodHandlers methodHandlers = allHandlers.next();
            if (methodHandlers != null) {
                validMethods.addAll(methodHandlers.getValidMethods());
            }
        }
        return validMethods;
    }

    private void handleFavicon(RestRequest.Method method, String uri, final RestChannel channel) {
        try {
            if (method != RestRequest.Method.GET) {
                handleUnsupportedHttpMethod(uri, method, channel, Set.of(RestRequest.Method.GET), null);
            } else {
                try {
                    try (InputStream stream = getClass().getResourceAsStream("/config/favicon.ico")) {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        Streams.copy(stream, out);
                        BytesRestResponse restResponse = new BytesRestResponse(RestStatus.OK, "image/x-icon", out.toByteArray());
                        channel.sendResponse(restResponse);
                    }
                } catch (IOException e) {
                    channel.sendResponse(
                        new BytesRestResponse(INTERNAL_SERVER_ERROR, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                }
            }
        } catch (final IllegalArgumentException e) {
            handleUnsupportedHttpMethod(uri, null, channel, Set.of(RestRequest.Method.GET), e);
        }
    }

    /**
     * 在普通的handler上包装了一层熔断器  当处理完毕时 减少熔断器的负载
     */
    private static final class ResourceHandlingHttpChannel implements RestChannel {
        private final RestChannel delegate;
        private final CircuitBreakerService circuitBreakerService;
        private final int contentLength;
        private final AtomicBoolean closed = new AtomicBoolean();

        ResourceHandlingHttpChannel(RestChannel delegate, CircuitBreakerService circuitBreakerService, int contentLength) {
            this.delegate = delegate;
            this.circuitBreakerService = circuitBreakerService;
            this.contentLength = contentLength;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return delegate.newBuilder();
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return delegate.newErrorBuilder();
        }

        @Override
        public XContentBuilder newBuilder(@Nullable XContentType xContentType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(xContentType, useFiltering);
        }

        @Override
        public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering)
                throws IOException {
            return delegate.newBuilder(xContentType, responseContentType, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public RestRequest request() {
            return delegate.request();
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return delegate.detailedErrorsEnabled();
        }

        @Override
        public void sendResponse(RestResponse response) {
            close();
            delegate.sendResponse(response);
        }

        /**
         * 就是在发送结果时 减少熔断器的负载
         */
        private void close() {
            // attempt to close once atomically
            if (closed.compareAndSet(false, true) == false) {
                throw new IllegalStateException("Channel is already closed");
            }
            inFlightRequestsBreaker(circuitBreakerService).addWithoutBreaking(-contentLength);
        }

    }

    private static CircuitBreaker inFlightRequestsBreaker(CircuitBreakerService circuitBreakerService) {
        // We always obtain a fresh breaker to reflect changes to the breaker configuration.
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }
}
