package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	taskspb "tasks/proto"
	"time"

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
	mu sync.RWMutex
	tasks []*taskspb.Task
	nextID int64
}

var clientPool = map[string]map[string]struct{}{
    "client1": {
        taskspb.TaskService_GetTask_FullMethodName:    {},
        taskspb.TaskService_CreateTask_FullMethodName: {},
		taskspb.TaskService_ListTasks_FullMethodName: {},
		taskspb.TaskService_UpdateTask_FullMethodName: {}, 
		taskspb.TaskService_WatchTasks_FullMethodName: {},
		taskspb.TaskService_UploadTasks_FullMethodName: {},
		taskspb.TaskService_SyncTasks_FullMethodName: {},
    },
    "client2": {
        taskspb.TaskService_GetTask_FullMethodName:    {},
        taskspb.TaskService_CreateTask_FullMethodName: {},
        taskspb.TaskService_DeleteTask_FullMethodName: {},
		taskspb.TaskService_ListTasks_FullMethodName: {},
		taskspb.TaskService_UpdateTask_FullMethodName: {}, 
		taskspb.TaskService_WatchTasks_FullMethodName: {},
		taskspb.TaskService_UploadTasks_FullMethodName: {},
		taskspb.TaskService_SyncTasks_FullMethodName: {},
    },
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
	
	task, ok := s.findTaskByID(id)
	if !ok {
		return nil, status.Error(codes.NotFound, "task with this id not found")
	}
	
	return task, nil
}

func (s *TaskServer) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	title, done := req.Title, req.Done
	
	if title == "" {
		return nil, status.Error(codes.InvalidArgument, "title cant be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	newTask := &taskspb.Task{Id: id, Title: title, Done: done}
	s.tasks = append(s.tasks, newTask)
	s.nextID++
	return &taskspb.Task{Id: newTask.Id, Title: newTask.Title, Done: newTask.Done}, nil
}

func (s *TaskServer) ListTasks(ctx context.Context, req *taskspb.ListTaskRequest) (*taskspb.ListTaskResponse, error) {
	s.mu.RLock()
	tasks := make([]*taskspb.Task, 0, len(s.tasks))
	defer s.mu.RUnlock()
	for _, task := range s.tasks {
		tasks = append(tasks, &taskspb.Task{Id: task.Id, Title: task.Title, Done: task.Done})
	}
	return &taskspb.ListTaskResponse{Tasks: tasks}, nil
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

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		if task.Id == req.Id {
			if req.Title != nil {
				task.Title = *req.Title
			}
			if req.Done != nil {
				task.Done = *req.Done
			}
			return &taskspb.Task{Id: task.Id, Title: task.Title, Done: task.Done}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "task with this id not found")
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
			log.Printf("send to the stream: %v", err)
			return err
		}
		log.Printf("task: %d id has been sent", task.Id)
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
					log.Printf("send and close stream: %v\n", err)
					return err
				}
				log.Printf("stream has been ended\n")
				return nil
			}
			log.Printf("receive task from client: %v\n", err)
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
				log.Printf("client stop sending\n")
				return nil
			}
			log.Printf("receive msg from client: %v\n", err)
			return err
		}
		taskResult := taskspb.TaskResult{Id: task.Id, Message: "task received"}
		err = stream.Send(&taskResult)
		if err != nil {
			log.Printf("send task result to stream: %v", err)
			return err
		}
		log.Printf("task result id: %d has been sent\n", taskResult.Id)
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

func loggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	requestIDString := ""
	commonName := ""
	start := time.Now()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Printf("metadata not found\n")
	}

	requestID := md.Get("request-id")
	if len(requestID) > 0 {
		requestIDString = requestID[0]
	}

	commonName, err := getCommonName(ctx)
	if err != nil {
		log.Printf("get common name: %v", err)
	}

	resp, err := handler(ctx, req)
	duration := time.Since(start)
	method := info.FullMethod
	log.Printf("method=%s duration=%s error=%v request-id=%s commonName:%s\n", method, duration, err, requestIDString, commonName)
	return resp, err
}

func loggingStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	requestIDString := ""
	start := time.Now()
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		log.Printf("no metadata\n")
	}

	requestID := md.Get("request-id")
	if len(requestID) > 0 {
		requestIDString = requestID[0]
	}
	
	commonName, err := getCommonName(ss.Context())
	if err != nil {
		log.Printf("get common name: %v", err)
	}

	err = handler(srv, ss)
	duration := time.Since(start)
	log.Printf("method=%s duration=%s error=%v is_client_stream=%t is_server_stream=%t request-id=%s commonName=%s\n", info.FullMethod, duration, err, info.IsClientStream, info.IsServerStream, requestIDString, commonName)
	return err
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
		ClientCAs: clientCAs,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	
	return credentials.NewTLS(tlsConfig), nil
}

func newGRPCServer(tc credentials.TransportCredentials) *grpc.Server {
	interceptor := grpc.ChainUnaryInterceptor(loggingInterceptor, authInterceptor)
	streamInterceptor := grpc.ChainStreamInterceptor(loggingStreamInterceptor, authStreamInterceptor) 

	tasks := []*taskspb.Task{
		{Id: 1, Title: "learn golang", Done: false},
		{Id: 2, Title: "learn grpc", Done: false},
		{Id: 3, Title: "learn html", Done: true},
	}
	
	server := grpc.NewServer(interceptor, streamInterceptor, grpc.Creds(tc))

	ts := &TaskServer{tasks: tasks, nextID: 4}
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

func main() {
	tc, err := loadServerTLSCredentials("certs/server.crt", "certs/server.key", "certs/ca.crt")
	if err != nil {
		log.Printf("load server tls credentials: %v", err)
		return
	}

	server := newGRPCServer(tc)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Printf("create listener: %v", err)
		return
	}

	sigChan := make(chan os.Signal, 1)
	

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("serve server and listener: %v\n", err)
			return
		}
	}()
	
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	
	done := make(chan struct{}, 1)
	go func() {
		server.GracefulStop()
		done<-struct{}{}
	}()

	select {
	case <-done:
		log.Printf("server has been gracefully stopped\n")
	case <-time.After(5*time.Second):
		log.Printf("5 sec is left, stop the server\n")
		server.Stop()
	}
}
