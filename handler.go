// Copyright 2021-2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect

import (
	"context"
	"net/http"
)

// A Handler is the server-side implementation of a single RPC defined by a
// service schema.
//
// By default, Handlers support the Connect, gRPC, and gRPC-Web protocols with
// the binary Protobuf and JSON codecs. They support gzip compression using the
// standard library's [compress/gzip].
type Handler struct {
	spec             Spec
	implementation   StreamingHandlerFunc
	protocolHandlers []protocolHandler
	acceptPost       string // Accept-Post header
}

// NewUnaryHandler constructs a [Handler] for a request-response procedure.
func NewUnaryHandler[Req, Res any](
	procedure string,
	unary func(context.Context, *Request[Req]) (*Response[Res], error),
	options ...HandlerOption,
) *Handler {
	// Wrap the strongly-typed implementation so we can apply interceptors.
	untyped := UnaryFunc(func(ctx context.Context, request AnyRequest) (AnyResponse, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		typed, ok := request.(*Request[Req])
		if !ok {
			return nil, errorf(CodeInternal, "unexpected handler request type %T", request)
		}
		return unary(ctx, typed)
	})
	config := newHandlerConfig(procedure, options)
	if interceptor := config.Interceptor; interceptor != nil {
		untyped = interceptor.WrapUnary(untyped)
	}
	// Given a stream, how should we call the unary function?
	implementation := func(ctx context.Context, conn StreamingHandlerConn) error {
		var msg Req
		if err := conn.Receive(&msg); err != nil {
			return err
		}
		request := &Request[Req]{
			Msg:    &msg,
			spec:   conn.Spec(),
			peer:   conn.Peer(),
			header: conn.RequestHeader(),
		}
		response, err := untyped(ctx, request)
		if err != nil {
			return err
		}
		mergeHeaders(conn.ResponseHeader(), response.Header())
		mergeHeaders(conn.ResponseTrailer(), response.Trailer())
		return conn.Send(response.Any())
	}

	protocolHandlers := config.newProtocolHandlers(StreamTypeUnary)
	return &Handler{
		spec:             config.newSpec(StreamTypeUnary),
		implementation:   implementation,
		protocolHandlers: protocolHandlers,
		acceptPost:       sortedAcceptPostValue(protocolHandlers),
	}
}

// NewClientStreamHandler constructs a [Handler] for a client streaming procedure.
func NewClientStreamHandler[Req, Res any](
	procedure string,
	implementation func(context.Context, *ClientStream[Req]) (*Response[Res], error),
	options ...HandlerOption,
) *Handler {
	return newStreamHandler(
		procedure,
		StreamTypeClient,
		func(ctx context.Context, conn StreamingHandlerConn) error {
			stream := &ClientStream[Req]{conn: conn}
			res, err := implementation(ctx, stream)
			if err != nil {
				return err
			}
			mergeHeaders(conn.ResponseHeader(), res.header)
			mergeHeaders(conn.ResponseTrailer(), res.trailer)
			return conn.Send(res.Msg)
		},
		options...,
	)
}

// NewServerStreamHandler constructs a [Handler] for a server streaming procedure.
func NewServerStreamHandler[Req, Res any](
	procedure string,
	implementation func(context.Context, *Request[Req], *ServerStream[Res]) error,
	options ...HandlerOption,
) *Handler {
	return newStreamHandler(
		procedure,
		StreamTypeServer,
		func(ctx context.Context, conn StreamingHandlerConn) error {
			var msg Req
			if err := conn.Receive(&msg); err != nil {
				return err
			}
			return implementation(
				ctx,
				&Request[Req]{
					Msg:    &msg,
					spec:   conn.Spec(),
					peer:   conn.Peer(),
					header: conn.RequestHeader(),
				},
				&ServerStream[Res]{conn: conn},
			)
		},
		options...,
	)
}

// NewBidiStreamHandler constructs a [Handler] for a bidirectional streaming procedure.
func NewBidiStreamHandler[Req, Res any](
	procedure string,
	implementation func(context.Context, *BidiStream[Req, Res]) error,
	options ...HandlerOption,
) *Handler {
	return newStreamHandler(
		procedure,
		StreamTypeBidi,
		func(ctx context.Context, conn StreamingHandlerConn) error {
			return implementation(
				ctx,
				&BidiStream[Req, Res]{conn: conn},
			)
		},
		options...,
	)
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// We don't need to defer functions  to close the request body or read to
	// EOF: the stream we construct later on already does that, and we only
	// return early when dealing with misbehaving clients. In those cases, it's
	// okay if we can't re-use the connection.
	isBidi := (h.spec.StreamType & StreamTypeBidi) == StreamTypeBidi
	if isBidi && request.ProtoMajor < 2 {
		responseWriter.WriteHeader(http.StatusHTTPVersionNotSupported)
		return
	}

	// The gRPC-HTTP2, gRPC-Web, and Connect protocols are all POST-only.
	if request.Method != http.MethodPost {
		responseWriter.Header().Set("Allow", http.MethodPost)
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Find our implementation of the RPC protocol in use.
	contentType := canonicalizeContentType(request.Header.Get("Content-Type"))
	var protocolHandler protocolHandler
	for _, handler := range h.protocolHandlers {
		if _, ok := handler.ContentTypes()[contentType]; ok {
			protocolHandler = handler
			break
		}
	}
	if protocolHandler == nil {
		responseWriter.Header().Set("Accept-Post", h.acceptPost)
		responseWriter.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	// Establish a stream and serve the RPC.
	request.Header.Set("Content-Type", contentType)                // prefer canonicalized value
	ctx, cancel, timeoutErr := protocolHandler.SetTimeout(request) //nolint: contextcheck
	if timeoutErr != nil {
		ctx = request.Context()
	}
	if cancel != nil {
		defer cancel()
	}
	connCloser, ok := protocolHandler.NewConn(
		responseWriter,
		request.WithContext(ctx),
	)
	if !ok {
		// Failed to create stream, usually because client used an unknown
		// compression algorithm. Nothing further to do.
		return
	}
	if timeoutErr != nil {
		_ = connCloser.Close(timeoutErr)
		return
	}
	_ = connCloser.Close(h.implementation(ctx, connCloser))
}

type handlerConfig struct {
	CompressionPools map[string]*compressionPool
	CompressionNames []string
	Codecs           map[string]Codec
	CompressMinBytes int
	Interceptor      Interceptor
	Procedure        string
	HandleGRPC       bool
	HandleGRPCWeb    bool
	BufferPool       *bufferPool
	ReadMaxBytes     int
	SendMaxBytes     int
}

func newHandlerConfig(procedure string, options []HandlerOption) *handlerConfig {
	protoPath := extractProtoPath(procedure)
	config := handlerConfig{
		Procedure:        protoPath,
		CompressionPools: make(map[string]*compressionPool),
		Codecs:           make(map[string]Codec),
		HandleGRPC:       true,
		HandleGRPCWeb:    true,
		BufferPool:       newBufferPool(),
	}
	withProtoBinaryCodec().applyToHandler(&config)
	withProtoJSONCodecs().applyToHandler(&config)
	withGzip().applyToHandler(&config)
	for _, opt := range options {
		opt.applyToHandler(&config)
	}
	return &config
}

func (c *handlerConfig) newSpec(streamType StreamType) Spec {
	return Spec{
		Procedure:  c.Procedure,
		StreamType: streamType,
	}
}

func (c *handlerConfig) newProtocolHandlers(streamType StreamType) []protocolHandler {
	protocols := []protocol{&protocolConnect{}}
	if c.HandleGRPC {
		protocols = append(protocols, &protocolGRPC{web: false})
	}
	if c.HandleGRPCWeb {
		protocols = append(protocols, &protocolGRPC{web: true})
	}
	handlers := make([]protocolHandler, 0, len(protocols))
	codecs := newReadOnlyCodecs(c.Codecs)
	compressors := newReadOnlyCompressionPools(
		c.CompressionPools,
		c.CompressionNames,
	)
	for _, protocol := range protocols {
		handlers = append(handlers, protocol.NewHandler(&protocolHandlerParams{
			Spec:             c.newSpec(streamType),
			Codecs:           codecs,
			CompressionPools: compressors,
			CompressMinBytes: c.CompressMinBytes,
			BufferPool:       c.BufferPool,
			ReadMaxBytes:     c.ReadMaxBytes,
			SendMaxBytes:     c.SendMaxBytes,
		}))
	}
	return handlers
}

func newStreamHandler(
	procedure string,
	streamType StreamType,
	implementation StreamingHandlerFunc,
	options ...HandlerOption,
) *Handler {
	config := newHandlerConfig(procedure, options)
	if ic := config.Interceptor; ic != nil {
		implementation = ic.WrapStreamingHandler(implementation)
	}
	protocolHandlers := config.newProtocolHandlers(streamType)
	return &Handler{
		spec:             config.newSpec(streamType),
		implementation:   implementation,
		protocolHandlers: protocolHandlers,
		acceptPost:       sortedAcceptPostValue(protocolHandlers),
	}
}
