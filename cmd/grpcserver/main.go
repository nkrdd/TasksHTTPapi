package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tasks/internal/logger"
	"tasks/internal/storage"
	taskspb "tasks/proto"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

type TaskServer struct {
	taskspb.UnimplementedTaskServiceServer
	store taskStore
	mu     sync.RWMutex
	tasks  []*taskspb.Task
	nextID int64
}

type taskStore interface {
	GetByID(ctx context.Context, id int64) (storage.Task, error)
	Create(ctx context.Context, title string, done bool) (storage.Task, error)
	List(ctx context.Context) ([]storage.Task, error)
	Update(ctx context.Context, id int64, title *string, done *bool) (storage.Task, error)
}

var clientPool = map[string]map[string]struct{}{
	"client1": {
		taskspb.TaskService_GetTask_FullMethodName:     {},
		taskspb.TaskService_CreateTask_FullMethodName:  {},
		taskspb.TaskService_ListTasks_FullMethodName:   {},
		taskspb.TaskService_UpdateTask_FullMethodName:  {},
		taskspb.TaskService_WatchTasks_FullMethodName:  {},
		taskspb.TaskService_UploadTasks_FullMethodName: {},
		taskspb.TaskService_SyncTasks_FullMethodName:   {},
	},
	"client2": {
		taskspb.TaskService_GetTask_FullMethodName:     {},
		taskspb.TaskService_CreateTask_FullMethodName:  {},
		taskspb.TaskService_DeleteTask_FullMethodName:  {},
		taskspb.TaskService_ListTasks_FullMethodName:   {},
		taskspb.TaskService_UpdateTask_FullMethodName:  {},
		taskspb.TaskService_WatchTasks_FullMethodName:  {},
		taskspb.TaskService_UploadTasks_FullMethodName: {},
		taskspb.TaskService_SyncTasks_FullMethodName:   {},
	},
}

var grpcLogLevels = map[codes.Code]slog.Level{
	codes.OK:              slog.LevelInfo,
	codes.InvalidArgument: slog.LevelWarn,
	codes.NotFound:        slog.LevelInfo,
	codes.Canceled:        slog.LevelInfo,
	codes.Internal:        slog.LevelError,
	codes.Unavailable:     slog.LevelError,
	codes.Unknown:         slog.LevelError,
}

func (s *TaskServer) findTaskByID(id int64) (*taskspb.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, task := range s.tasks {
		if task.Id == id {
			taskCopy := &taskspb.Task{Id: task.Id, Title: task.Title, Done: task.Done}
			return taskCopy, true
		}
	}
	return nil, false
}

func (s *TaskServer) GetTask(ctx context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	id := req.Id

	if id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be positive")
	}

	task, err := s.store.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, "get task internal error")
	}

	return &taskspb.Task{
		Id:    task.ID,
		Title: task.Title,
		Done:  task.Done,
	}, nil
}

func (s *TaskServer) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	title, done := req.Title, req.Done

	if title == "" {
		return nil, status.Error(codes.InvalidArgument, "title cant be empty")
	}
	
	task, err := s.store.Create(ctx, title, done)
	if err != nil {
		return nil, status.Error(codes.Internal, "create task in db error")
	}

	createdTask := &taskspb.Task{Id: task.ID, Title: task.Title, Done: task.Done}
	return createdTask, nil
}

func (s *TaskServer) ListTasks(ctx context.Context, req *taskspb.ListTaskRequest) (*taskspb.ListTaskResponse, error) {
	tasks, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "storage error while list task func")
	}
	
	resp := &taskspb.ListTaskResponse{Tasks: make([]*taskspb.Task, 0, len(tasks))}
	
	for _, task := range tasks {
		resp.Tasks = append(resp.Tasks, &taskspb.Task{Id: task.ID, Title: task.Title, Done: task.Done})	
	}

	return resp, nil
}

func (s *TaskServer) UpdateTask(ctx context.Context, req *taskspb.UpdateTaskRequest) (*taskspb.Task, error) {
	if req.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be positive")
	}

	if req.Title == nil && req.Done == nil {
		return nil, status.Error(codes.InvalidArgument, "one of optional params must be include")
	}

	if req.Title != nil && *req.Title == "" {
		return nil, status.Error(codes.InvalidArgument, "title must be not empty")
	}
	
	updatedTask, err := s.store.Update(ctx, req.Id, req.Title, req.Done)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil,	status.Error(codes.NotFound, "task with this id not found")
		}
		return nil, status.Error(codes.Internal, "unexpected error") 
	}
	
	return &taskspb.Task{Id: updatedTask.ID, Title: updatedTask.Title, Done: updatedTask.Done}, nil
}

func (s *TaskServer) DeleteTask(ctx context.Context, req *taskspb.DeleteTaskRequest) (*taskspb.Task, error) {
	if req.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, task := range s.tasks {
		if task.Id == req.Id {
			copiedTask := &taskspb.Task{Id: task.Id, Title: task.Title, Done: task.Done}
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			return copiedTask, nil
		}
	}
	return nil, status.Error(codes.NotFound, "task with this id not found")
}

func (s *TaskServer) WatchTasks(req *taskspb.WatchTaskRequest, stream grpc.ServerStreamingServer[taskspb.Task]) error {
	s.mu.RLock()
	tasks := make([]*taskspb.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, &taskspb.Task{Id: task.Id, Title: task.Title, Done: task.Done})
	}
	s.mu.RUnlock()

	for _, task := range tasks {
		if err := stream.Send(task); err != nil {
			return err
		}
	}
	return nil
}

func (s *TaskServer) UploadTasks(stream grpc.ClientStreamingServer[taskspb.Task, taskspb.UploadTasksResponse]) error {
	var processed int32
	for {
		_, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				err := stream.SendAndClose(&taskspb.UploadTasksResponse{Processed: processed})
				if err != nil {
					return err
				}
				return nil
			}
			return err
		}
		processed++
	}
}

func (s *TaskServer) SyncTasks(stream grpc.BidiStreamingServer[taskspb.Task, taskspb.TaskResult]) error {
	for {
		task, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		taskResult := taskspb.TaskResult{Id: task.Id, Message: "task received"}
		err = stream.Send(&taskResult)
		if err != nil {
			return err
		}
	}
}

func checkAccess(ctx context.Context, method string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "metadata not found")
	}

	auth := md.Get("authorization")
	if len(auth) == 0 {
		return status.Error(codes.Unauthenticated, "auth token not found")
	}
	if auth[0] != "sfadfsf3423fdas2" {
		return status.Error(codes.Unauthenticated, "not expected auth token")
	}

	commonName, err := getCommonName(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, "cant identify client personality")
	}

	if !canCall(commonName, method) {
		return status.Error(codes.PermissionDenied, fmt.Sprintf("%s cant call %s", commonName, method))
	}

	return nil
}

func authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == grpc_health_v1.Health_Check_FullMethodName {
		return handler(ctx, req)
	}

	if err := checkAccess(ctx, info.FullMethod); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func getCommonName(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.NotFound, "peer information not exist")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", status.Error(codes.FailedPrecondition, "types must be matched")
	}

	state := tlsInfo.State
	peerCert := state.PeerCertificates
	if len(peerCert) == 0 {
		return "", fmt.Errorf("PeerCertificates slice is empty")
	}
	clientCert := peerCert[0]
	commonName := clientCert.Subject.CommonName

	return commonName, nil
}

type requestIDKey struct{}

func requestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var requestID string

		md, _ := metadata.FromIncomingContext(ctx)
		ids := md.Get("request-id")

		if len(ids) > 0 && ids[0] != "" {
			requestID = ids[0]
		} else {
			requestID = uuid.NewString()
		}

		ctx = context.WithValue(ctx, requestIDKey{}, requestID)

		return handler(ctx, req)
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

func requestIDStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		var requestID string
		md, _ := metadata.FromIncomingContext(ctx)
		ids := md.Get("request-id")

		if len(ids) > 0 && ids[0] != "" {
			requestID = ids[0]
		} else {
			requestID = uuid.NewString()
		}

		ctx = context.WithValue(ctx, requestIDKey{}, requestID)

		newSS := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		return handler(srv, newSS)
	}
}

func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		requestIDString, ok := ctx.Value(requestIDKey{}).(string)
		if !ok {
			logger.Warn("get request id error")
		}

		commonName, err := getCommonName(ctx)
		if err != nil {
			logger.Warn("get common name", slog.Any("error", err))
		}

		resp, err := handler(ctx, req)
		grpcCode := status.Code(err)
		duration := time.Since(start)
		method := info.FullMethod

		level, ok := grpcLogLevels[grpcCode]
		if !ok {
			level = slog.LevelWarn
		}

		msg := "grpc request completed"

		attrs := []slog.Attr{
			slog.String("method", method),
			slog.Duration("duration", duration),
			slog.String("request_id", requestIDString),
			slog.String("common_name", commonName),
			slog.String("grpc_code", grpcCode.String()),
		}

		if err != nil {
			attrs = append(attrs, slog.Any("error", err))
		}

		logger.LogAttrs(ctx, level, msg, attrs...)

		return resp, err
	}
}

func loggingStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		requestIDString, ok := ss.Context().Value(requestIDKey{}).(string)
		if !ok {
			logger.Warn("get request id error")
		}

		commonName, err := getCommonName(ss.Context())
		if err != nil {
			logger.Warn("get common name", slog.Any("error", err))
		}

		err = handler(srv, ss)
		method := info.FullMethod
		isClientStream := info.IsClientStream
		isServerStream := info.IsServerStream
		grpcCode := status.Code(err)
		duration := time.Since(start)

		level, ok := grpcLogLevels[grpcCode]
		if !ok {
			level = slog.LevelWarn
		}

		msg := "grpc request completed"

		attrs := []slog.Attr{
			slog.String("method", method),
			slog.Duration("duration", duration),
			slog.String("request_id", requestIDString),
			slog.String("common_name", commonName),
			slog.Bool("is_client_stream", isClientStream),
			slog.Bool("is_server_stream", isServerStream),
			slog.String("grpc_code", grpcCode.String()),
		}

		if err != nil {
			attrs = append(attrs, slog.Any("error", err))
		}

		logger.LogAttrs(ss.Context(), level, msg, attrs...)

		return err
	}
}

func authStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod == reflectionv1.ServerReflection_ServerReflectionInfo_FullMethodName {
		return handler(srv, ss)
	}

	if err := checkAccess(ss.Context(), info.FullMethod); err != nil {
		return err
	}

	return handler(srv, ss)
}

func loadServerTLSCredentials(certPath, keyPath, caPath string) (credentials.TransportCredentials, error) {
	serverCert, err := tls.LoadX509KeyPair(
		certPath,
		keyPath,
	)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}

	clientCAs := x509.NewCertPool()

	if ok := clientCAs.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("append CA certificate error")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	return credentials.NewTLS(tlsConfig), nil
}

func newGRPCServer(tc credentials.TransportCredentials, logger *slog.Logger, storage taskStore) *grpc.Server {
	interceptor := grpc.ChainUnaryInterceptor(requestIDInterceptor(), loggingInterceptor(logger), authInterceptor)
	streamInterceptor := grpc.ChainStreamInterceptor(requestIDStreamInterceptor(), loggingStreamInterceptor(logger), authStreamInterceptor)

	tasks := []*taskspb.Task{
		{Id: 1, Title: "learn golang", Done: false},
		{Id: 2, Title: "learn grpc", Done: false},
		{Id: 3, Title: "learn html", Done: true},
	}

	server := grpc.NewServer(interceptor, streamInterceptor, grpc.Creds(tc))

	ts := &TaskServer{tasks: tasks, nextID: 4, store: storage}
	taskspb.RegisterTaskServiceServer(server, ts)

	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	grpc_health_v1.RegisterHealthServer(server, hs)
	reflection.Register(server)

	return server
}

func canCall(clientID, method string) bool {
	if clientRights, ok := clientPool[clientID]; ok {
		_, allowed := clientRights[method]
		return allowed
	}

	return false
}

func run() error {
	serverLogger := logger.New("server")

	dbURL := os.Getenv("DATABASE_URL")

	if dbURL == "" {
		serverLogger.Error("db url is empty")
		return errors.New("db url is empty")
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		serverLogger.Error(
			"failed to create connection pool",
			"error", err,
		)
		return err
	}
	defer pool.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()

	err = pool.Ping(pingCtx)
	if err != nil {
		serverLogger.Error(
			"failed to ping pool",
			"error", err,
		)
		return err
	}

	store := storage.New(pool)

	tc, err := loadServerTLSCredentials("certs/server.crt", "certs/server.key", "certs/ca.crt")
	if err != nil {
		serverLogger.Error(
			"failed to load server TLS credentials",
			slog.Any("error", err),
		)
		return err
	}

	server := newGRPCServer(tc, serverLogger, store)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		serverLogger.Error(
			"failed to create listener",
			slog.Any("error", err),
		)
		return err
	}

	sigChan := make(chan os.Signal, 1)

	serveErrch := make(chan error, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		serveErrch <- server.Serve(listener)
	}()

	var runErr error

	select {
	case <-sigChan:
		serverLogger.Info("shutdown requested")
	case runErr = <-serveErrch:
		if runErr == nil {
			runErr = errors.New("unexpected nil error in serve")
		}
		serverLogger.Error("server stopped with unexpected error", slog.Any("error", runErr))
	}

	done := make(chan struct{}, 1)
	go func() {
		server.GracefulStop()
		done <- struct{}{}
	}()

	select {
	case <-done:
		serverLogger.Info("server has been gracefully stopped")
	case <-time.After(5 * time.Second):
		serverLogger.Warn("gracefull shutdown exceeded 5 sec, Stop() was needed")
		server.Stop()
	}

	return runErr
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}
