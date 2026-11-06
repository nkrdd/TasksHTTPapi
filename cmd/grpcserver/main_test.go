package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"tasks/internal/storage"
	taskspb "tasks/proto"
	"testing"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeStore struct {
	tasks map[int64]storage.Task
	nextID int64
	err error
}

func (f *fakeStore) GetByID(ctx context.Context, id int64) (storage.Task, error) {
	if f.err != nil {
		return storage.Task{}, f.err 
	}
	
	task, ok := f.tasks[id]
	if !ok {
		return storage.Task{}, pgx.ErrNoRows
	}

	return task, nil 
}

func (f *fakeStore) Create(ctx context.Context, title string, done bool) (storage.Task, error) {
	if f.err != nil {
		return storage.Task{}, f.err
	}

	createdTask := storage.Task{ID: f.nextID, Title: title, Done: done}
	f.tasks[f.nextID] = createdTask
	f.nextID++

	return createdTask, nil
}

func (f *fakeStore) List(ctx context.Context) ([]storage.Task, error) {
	if f.err != nil {
		return nil, f.err
	}
	
	tasks := make([]storage.Task, 0, len(f.tasks))
	for _, task := range f.tasks {
		tasks = append(tasks, task)
	}

	sort.Slice(tasks, func(i, j int) bool {return tasks[i].ID < tasks[j].ID})

	return tasks, nil
}

func (f *fakeStore) Update(ctx context.Context, id int64, title *string, done *bool) (storage.Task, error) {
	if f.err != nil {
		return storage.Task{}, f.err
	}
	
	changedTask, ok := f.tasks[id]
	if !ok {
		return storage.Task{}, pgx.ErrNoRows
	}

	if title != nil {
		changedTask.Title = *title
	}

	if done != nil {
		changedTask.Done = *done
	}
	
	f.tasks[id] = changedTask
	
	return changedTask, nil
}

func TestGetTask(t *testing.T) {
	store := &fakeStore{
		tasks: map[int64]storage.Task{
			1: {ID: 1, Title: "Learn Go", Done: false},
		},
	}

	server := &TaskServer{
		store: store,
	}

	task, err := server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 1})
	if err != nil {
		t.Fatal("exepcted nil error, got:", err)
	}
	if task.Id != 1 || task.Title != store.tasks[1].Title || task.Done != store.tasks[1].Done {
		t.Fatal("unexpected task", task.Id, task.Title, task.Done)
	}

	task, err = server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 999})
	if status.Code(err) != codes.NotFound {
		t.Fatal("expected NotFound code, got:", status.Code(err))
	}
	
	task, err = server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: -1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("expected status code: invalidargument, got:", status.Code(err))
	}

	store.err = errors.New("database unavailable")	
	task, err = server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 1})
	if status.Code(err) != codes.Internal {
		t.Fatal("expected internal code error, got:", status.Code(err))
	}
}

func TestCreateTask(t *testing.T) {	
	store := &fakeStore{
		nextID: 2,
		tasks: map[int64]storage.Task{
			1: {ID: 1, Title: "Learn Go", Done: false},
		},
	}

	server := &TaskServer{
		store: store,
	}
	
	task, err := server.CreateTask(context.Background(), &taskspb.CreateTaskRequest{Title: "test task", Done: true})
	if err != nil {
		t.Fatal("expected nil error while creating task, got", err)
	}

	gettingTask, err := server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 2}) 
	if err != nil {
		t.Fatal("expected nil error while getting created task, got:", err)
	}

	if !proto.Equal(gettingTask, task) {
		t.Fatal("task doesnt match")
	}

	task, err = server.CreateTask(context.Background(), &taskspb.CreateTaskRequest{Title: "", Done: true})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("expected invalidArgument, got:", status.Code(err))
	}
	
	store.err = errors.New("database unavailable")	
	task, err = server.CreateTask(context.Background(), &taskspb.CreateTaskRequest{Title: "test task", Done: true})
	if status.Code(err) != codes.Internal {
		t.Fatal("expected internal error, got:", status.Code(err))
	} 
}

func TestListTasks(t *testing.T) {	
	store := &fakeStore{
		nextID: 4,
		tasks: map[int64]storage.Task{
			1: {ID: 1, Title: "Learn Go", Done: false},
			2: {ID: 2, Title: "Learn html", Done: true},
			3: {ID: 3, Title: "Just do it", Done: false},
		},
	}

	server := &TaskServer{
		store: store,
	}

	resp, err := server.ListTasks(context.Background(), &taskspb.ListTaskRequest{}) 
	if err != nil {
		t.Fatal("expected nil error, got", err)
	}
	
	if len(resp.Tasks) != len(store.tasks) {
		t.Fatalf("expected %d tasks, got %d", len(store.tasks), len(resp.Tasks))
	}
	
	seen := make(map[int64]bool)

	for _, got := range resp.Tasks {
		if seen[got.Id] {
			t.Fatal("this task already checked")
		}

		expected, ok := store.tasks[got.Id]

		if !ok {
			t.Fatalf("unexpected task id: %d", got.Id)
		}

		if got.Id != expected.ID ||
			got.Title != expected.Title ||
			got.Done != expected.Done {
				t.Fatalf("task mismatch: got %+v, expected %+v", got, expected)
			}
		seen[got.Id] = true
	}

	for i := 1; i < len(resp.Tasks); i++ {
		if resp.Tasks[i-1].Id >= resp.Tasks[i].Id {
			t.Fatal("tasks are not ordered by ID")
		}
	}

	store.tasks = make(map[int64]storage.Task)

	resp, err = server.ListTasks(context.Background(), &taskspb.ListTaskRequest{})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	if resp == nil {
		t.Fatal("expected resp not nil, got nil")
	}

	if len(resp.Tasks) != 0 {
		t.Fatal("expected empty array, got", len(resp.Tasks))
	}
	
	createdTask, err := server.CreateTask(context.Background(), &taskspb.CreateTaskRequest{Title: "test task", Done: true})
	if err != nil {
		t.Fatal("created task error", err)
	}

	resp, err = server.ListTasks(context.Background(), &taskspb.ListTaskRequest{})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(resp.Tasks))
	}

	if !proto.Equal(resp.Tasks[0], createdTask) {
		t.Fatal("created task not in list")
	}

	store.err = errors.New("database unavailable")
	resp, err = server.ListTasks(context.Background(), &taskspb.ListTaskRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatal("expected internal error, got", status.Code(err))
	}
}

func TestUpdateTask(t *testing.T) {
	store := &fakeStore{
		nextID: 4,
		tasks: map[int64]storage.Task{
			1: {ID: 1, Title: "Learn Go", Done: false},
			2: {ID: 2, Title: "Learn html", Done: true},
			3: {ID: 3, Title: "Just do it", Done: false},
		},
	}

	server := &TaskServer{
		store: store,
	}
	
	done := false
	task, err := server.UpdateTask(context.Background(), &taskspb.UpdateTaskRequest{Id: 2, Done: &done})
	if err != nil {
		t.Fatal("expected nil error, got:", err)
	}
	
	updatedTask, err := server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 2})
	if err != nil {
		t.Fatal("unexpected error while getting task")
	}

	if !proto.Equal(task, updatedTask) {
		t.Fatal("task hasnt been updated")
	}
	
	title := "changed"
	task, err = server.UpdateTask(context.Background(), &taskspb.UpdateTaskRequest{Id: 3, Title: &title})
	if err != nil {
		t.Fatal("expected nil error, got:", err)
	}

	updatedTask, err = server.GetTask(context.Background(), &taskspb.GetTaskRequest{Id: 3})
	if err != nil {
		t.Fatal("unexpected error while getting task")
	}

	if !proto.Equal(task, updatedTask) {
		t.Fatal("task hasnt been updated")
	}

	task, err = server.UpdateTask(context.Background(), &taskspb.UpdateTaskRequest{Id: 999, Title: &title})
	if status.Code(err) != codes.NotFound {
		t.Fatal("expected notfound code error, got:", status.Code(err))
	}

	_, err = server.UpdateTask(context.Background(), &taskspb.UpdateTaskRequest{Id: -1, Title: &title})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("expected code: invalid argument, got:", status.Code(err))
	}

	store.err = errors.New("database unavailable")
	_, err = server.UpdateTask(context.Background(), &taskspb.UpdateTaskRequest{Id: 1, Title: &title})
	if status.Code(err) != codes.Internal {
		t.Fatal("expected internal error, got:", status.Code(err))
	}
}

func TestGetCommonName_EmptyContext(t *testing.T) {
	ctx := context.Background()

	_, err := getCommonName(ctx)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetCommonName_ValidCertificate(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client1",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)

	commonName, err := getCommonName(ctx)
	if err != nil {
		t.Fatalf("expected: not error, got: %v", err)
	}

	if commonName != "client1" {
		t.Fatalf("expected commonName: client1, got: %s", commonName)
	}
}

func TestGetCommonName_PeerCertIsEmpty(t *testing.T) {
	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)

	commonName, err := getCommonName(ctx)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if commonName != "" {
		t.Fatalf("expected empty commonName, got %s", commonName)
	}
}

func TestAuthInterceptor_PermDenied(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client1",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas2",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.UnaryServerInfo{
		FullMethod: taskspb.TaskService_DeleteTask_FullMethodName,
	}

	called := false

	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := authInterceptor(ctx, nil, info, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	if called {
		t.Fatalf("handler must not be called")
	}
}

func TestAuthInterceptor_Success(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client1",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas2",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.UnaryServerInfo{
		FullMethod: taskspb.TaskService_GetTask_FullMethodName,
	}

	called := false

	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := authInterceptor(ctx, nil, info, handler)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if !called {
		t.Fatalf("handler must be called")
	}
}

func TestAuthInterceptor_InvalidToken(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client1",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.UnaryServerInfo{
		FullMethod: taskspb.TaskService_DeleteTask_FullMethodName,
	}

	called := false

	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := authInterceptor(ctx, nil, info, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}

	if called {
		t.Fatalf("handler must not be called")
	}
}

func TestAuthInterceptor_SuccessClient2(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client2",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas2",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.UnaryServerInfo{
		FullMethod: taskspb.TaskService_DeleteTask_FullMethodName,
	}

	called := false

	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := authInterceptor(ctx, nil, info, handler)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if !called {
		t.Fatalf("handler must be called")
	}
}

func TestCanCallFunc_Client1(t *testing.T) {
	if !canCall("client1", taskspb.TaskService_GetTask_FullMethodName) {
		t.Fatalf("client1 can call get task method!")
	}

	if canCall("client1", taskspb.TaskService_DeleteTask_FullMethodName) {
		t.Fatalf("client1 cant call delete method")
	}

	if !canCall("client1", taskspb.TaskService_CreateTask_FullMethodName) {
		t.Fatalf("client1 can call create task method")
	}
}

func TestCanCallFunc_Client2(t *testing.T) {
	if !canCall("client2", taskspb.TaskService_GetTask_FullMethodName) {
		t.Fatalf("client2 can call get task method!")
	}

	if !canCall("client2", taskspb.TaskService_DeleteTask_FullMethodName) {
		t.Fatalf("client2 can call delete method")
	}

	if !canCall("client2", taskspb.TaskService_CreateTask_FullMethodName) {
		t.Fatalf("client2 can call create task method")
	}
}

func TestCanCallFunc_Client3(t *testing.T) {
	if canCall("client3", taskspb.TaskService_GetTask_FullMethodName) {
		t.Fatalf("client3 cant call get task method")
	}
}

func TestCanCallFunc_Client2_NotExistMethod(t *testing.T) {
	if canCall("client2", "lol") {
		t.Fatalf("cant call not exist rpc")
	}
}

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) Context() context.Context {
	return s.ctx
}

func TestAuthStreamInterceptor_PermDenied(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client3",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas2",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.StreamServerInfo{
		FullMethod: taskspb.TaskService_WatchTasks_FullMethodName,
	}

	called := false

	handler := func(srv any, ss grpc.ServerStream) error {
		called = true
		return nil
	}

	ss := testServerStream{
		ctx: ctx,
	}

	err := authStreamInterceptor(nil, &ss, info, handler)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	if called {
		t.Fatalf("handler must not be called")
	}
}

func TestAuthStreamInterceptor_Success(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client2",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx := peer.NewContext(context.Background(), &p)
	md := metadata.Pairs(
		"authorization", "sfadfsf3423fdas2",
	)

	ctx = metadata.NewIncomingContext(ctx, md)
	info := &grpc.StreamServerInfo{
		FullMethod: taskspb.TaskService_WatchTasks_FullMethodName,
	}

	called := false

	handler := func(srv any, ss grpc.ServerStream) error {
		called = true
		return nil
	}

	ss := testServerStream{
		ctx: ctx,
	}

	err := authStreamInterceptor(nil, &ss, info, handler)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if !called {
		t.Fatalf("handler must be called")
	}
}

func TestLoggingInterceptor(t *testing.T) {
	ctx := context.Background()

	ctx = context.WithValue(
		ctx,
		requestIDKey{},
		"test-request-123",
	)

	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client2",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx = peer.NewContext(ctx, &p)

	info := &grpc.UnaryServerInfo{
		FullMethod: taskspb.TaskService_GetTask_FullMethodName,
	}

	tests := []struct {
		name          string
		handlerErr    error
		expectedCode  codes.Code
		expectedLevel slog.Level
		expectError   bool
	}{
		{
			name:          "success",
			handlerErr:    nil,
			expectedCode:  codes.OK,
			expectedLevel: slog.LevelInfo,
			expectError:   false,
		},
		{
			name:          "invalid argument",
			handlerErr:    status.Error(codes.InvalidArgument, "invalid id"),
			expectedCode:  codes.InvalidArgument,
			expectedLevel: slog.LevelWarn,
			expectError:   true,
		},
		{
			name:          "internal failure",
			handlerErr:    status.Error(codes.Internal, "internal failure"),
			expectedCode:  codes.Internal,
			expectedLevel: slog.LevelError,
			expectError:   true,
		},
		{
			name:          "not found",
			handlerErr:    status.Error(codes.NotFound, "not found"),
			expectedCode:  codes.NotFound,
			expectedLevel: slog.LevelInfo,
			expectError:   true,
		},
		{
			name:          "unknown",
			handlerErr:    errors.New("unexpected error"),
			expectedCode:  codes.Unknown,
			expectedLevel: slog.LevelError,
			expectError:   true,
		},
		{
			name:          "permission denied",
			handlerErr:    status.Error(codes.PermissionDenied, "permission denied"),
			expectedCode:  codes.PermissionDenied,
			expectedLevel: slog.LevelWarn,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			var expectedResp any

			if tt.handlerErr == nil {
				expectedResp = &taskspb.Task{
					Id:    42,
					Title: "testTask",
				}
			}

			logger := slog.New(slog.NewJSONHandler(&buf, nil))
			interceptor := loggingInterceptor(logger)

			called := false

			handler := func(ctx context.Context, req any) (any, error) {
				called = true
				return expectedResp, tt.handlerErr
			}

			resp, err := interceptor(ctx, nil, info, handler)

			if resp != expectedResp {
				t.Fatalf("response was modified by interceptor")
			}

			if status.Code(err) != tt.expectedCode {
				t.Fatalf("expected code: %s, got %s", tt.expectedCode, status.Code(err))
			}

			if !called {
				t.Fatalf("handler not called")
			}

			var record map[string]any

			err = json.Unmarshal(buf.Bytes(), &record)
			if err != nil {
				t.Fatal("parse json:", err)
			}

			if record["grpc_code"] != tt.expectedCode.String() {
				t.Fatalf("expected status_code: %s, got: %s", tt.expectedCode.String(), record["grpc_code"])
			}

			if record["request_id"] != ctx.Value(requestIDKey{}) {
				t.Fatalf("expected request_id: %s, got: %s", ctx.Value(requestIDKey{}), record["request_id"])
			}

			if record["common_name"] != cert.Subject.CommonName {
				t.Fatalf("expected common_name: %s, got: %s", cert.Subject.CommonName, record["common_name"])
			}

			if record["level"] != tt.expectedLevel.String() {
				t.Fatalf("expected level: %s, got: %s", tt.expectedLevel.String(), record["level"])
			}

			if record["method"] != info.FullMethod {
				t.Fatalf("expected method: %s, got: %s", info.FullMethod, record["method"])
			}

			if record["msg"] != "grpc request completed" {
				t.Fatalf("unexpected msg: %v", record["msg"])
			}

			if _, ok := record["duration"]; !ok {
				t.Fatalf("cannot find duration in log")
			}

			if _, ok := record["error"]; ok != tt.expectError {
				t.Fatalf("expect err: %t got: %t", tt.expectError, ok)
			}
		})
	}
}

func TestLoggingStreamInterceptor(t *testing.T) {
	ctx := context.Background()

	ctx = context.WithValue(
		ctx,
		requestIDKey{},
		"test-request-123",
	)

	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client2",
		},
	}

	connState := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	authInfo := credentials.TLSInfo{State: connState}
	p := peer.Peer{AuthInfo: authInfo}
	ctx = peer.NewContext(ctx, &p)

	ss := testServerStream{
		ctx: ctx,
	}

	tests := []struct {
		name                 string
		handlerErr           error
		expectedCode         codes.Code
		expectedLevel        slog.Level
		expectError          bool
		method               string
		expectedClientStream bool
		expectedServerStream bool
	}{
		{
			name:                 "success watch tasks",
			handlerErr:           nil,
			expectedCode:         codes.OK,
			expectedLevel:        slog.LevelInfo,
			expectError:          false,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "invalid argument",
			handlerErr:           status.Error(codes.InvalidArgument, "invalid id"),
			expectedCode:         codes.InvalidArgument,
			expectedLevel:        slog.LevelWarn,
			expectError:          true,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "internal failure",
			handlerErr:           status.Error(codes.Internal, "internal failure"),
			expectedCode:         codes.Internal,
			expectedLevel:        slog.LevelError,
			expectError:          true,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "not found",
			handlerErr:           status.Error(codes.NotFound, "not found"),
			expectedCode:         codes.NotFound,
			expectedLevel:        slog.LevelInfo,
			expectError:          true,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "unknown",
			handlerErr:           errors.New("unexpected error"),
			expectedCode:         codes.Unknown,
			expectedLevel:        slog.LevelError,
			expectError:          true,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "permission denied",
			handlerErr:           status.Error(codes.PermissionDenied, "permission denied"),
			expectedCode:         codes.PermissionDenied,
			expectedLevel:        slog.LevelWarn,
			expectError:          true,
			method:               taskspb.TaskService_WatchTasks_FullMethodName,
			expectedClientStream: false,
			expectedServerStream: true,
		},
		{
			name:                 "success upload tasks",
			handlerErr:           nil,
			expectedCode:         codes.OK,
			expectedLevel:        slog.LevelInfo,
			expectError:          false,
			method:               taskspb.TaskService_UploadTasks_FullMethodName,
			expectedClientStream: true,
			expectedServerStream: false,
		},
		{
			name:                 "success sync tasks",
			handlerErr:           nil,
			expectedCode:         codes.OK,
			expectedLevel:        slog.LevelInfo,
			expectError:          false,
			method:               taskspb.TaskService_SyncTasks_FullMethodName,
			expectedClientStream: true,
			expectedServerStream: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			info := &grpc.StreamServerInfo{
				FullMethod:     tt.method,
				IsClientStream: tt.expectedClientStream,
				IsServerStream: tt.expectedServerStream,
			}

			called := false

			handler := func(srv any, ss grpc.ServerStream) error {
				called = true
				return tt.handlerErr
			}

			logger := slog.New(slog.NewJSONHandler(&buf, nil))
			interceptor := loggingStreamInterceptor(logger)

			err := interceptor(nil, &ss, info, handler)

			if !called {
				t.Fatalf("handler not called")
			}

			if status.Code(err) != tt.expectedCode {
				t.Fatalf("expected code: %s, got %s", tt.expectedCode, status.Code(err))
			}

			var record map[string]any

			err = json.Unmarshal(buf.Bytes(), &record)
			if err != nil {
				t.Fatal("parse json:", err)
			}

			if record["grpc_code"] != tt.expectedCode.String() {
				t.Fatalf("expected status_code: %s, got: %s", tt.expectedCode.String(), record["grpc_code"])
			}

			if record["request_id"] != ctx.Value(requestIDKey{}) {
				t.Fatalf("expected request_id: %s, got: %s", ctx.Value(requestIDKey{}), record["request_id"])
			}

			if record["common_name"] != cert.Subject.CommonName {
				t.Fatalf("expected common_name: %s, got: %s", cert.Subject.CommonName, record["common_name"])
			}

			if record["level"] != tt.expectedLevel.String() {
				t.Fatalf("expected level: %s, got: %s", tt.expectedLevel.String(), record["level"])
			}

			if record["method"] != info.FullMethod {
				t.Fatalf("expected method: %s, got: %s", info.FullMethod, record["method"])
			}

			if record["msg"] != "grpc request completed" {
				t.Fatalf("unexpected msg: %v", record["msg"])
			}

			if _, ok := record["duration"]; !ok {
				t.Fatalf("cannot find duration in log")
			}

			if _, ok := record["error"]; ok != tt.expectError {
				t.Fatalf("expect err: %t got: %t", tt.expectError, ok)
			}

			if record["is_client_stream"] != tt.expectedClientStream {
				t.Fatalf("expected is_client_stream: %t got: %t", info.IsClientStream, record["is_client_stream"])
			}

			if record["is_server_stream"] != tt.expectedServerStream {
				t.Fatalf("expected is_server_stream: %t, got: %t", info.IsServerStream, record["is_server_stream"])
			}
		})
	}
}
