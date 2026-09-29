package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"os"
	"path/filepath"
	taskspb "tasks/proto"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	certDir := filepath.Join("..", "..", "certs")
	tc, err := loadServerTLSCredentials(filepath.Join(certDir, "server.crt"), filepath.Join(certDir, "server.key"), filepath.Join(certDir, "ca.crt"))
	if err != nil {
		t.Fatalf("load server tls credentials failed: %v", err)
	}

	server := newGRPCServer(tc)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen port failed: %v", err)
	}
	
	serveErrCh := make(chan error, 1)

	go func() {
		serveErrCh <- server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()

		if err := <-serveErrCh; err != nil {
			t.Errorf("gRPC server failed: %v", err)
		}
	})

	return listener.Addr().String()
}

func newTestClient(t *testing.T, serverAddr, certFile, keyFile string) taskspb.TaskServiceClient {
	t.Helper()

	certDir := filepath.Join("..", "..", "certs")	
	clientCert := make([]tls.Certificate, 0)

	caPEM, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		t.Fatalf("read ca certificate: %v", err)
	}

	certCAPool := x509.NewCertPool()

	ok := certCAPool.AppendCertsFromPEM(caPEM)
	if !ok {
		t.Fatalf("append certs from pem cause with error")
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(
			filepath.Join(certDir, certFile),
			filepath.Join(certDir, keyFile),
		)
		if err != nil {
			t.Fatalf("load client certificate: %v", err)
		}

		clientCert = append(clientCert, cert)
	}
	
	if (certFile == "" && keyFile != "") || (keyFile == "" && certFile != "") {
		t.Fatalf("client config load error")
	}

	tlsConfig := tls.Config{
		Certificates: clientCert, 
		RootCAs: certCAPool,
		ServerName: "localhost",
	}	

	tcClient := credentials.NewTLS(&tlsConfig)

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(tcClient),)
	if err != nil {
		t.Fatalf("create new client connection: %v", err)
	}

	tsc := taskspb.NewTaskServiceClient(conn)
	
	t.Cleanup(func() {
		conn.Close()
	})

	return tsc
}

func TestMTLSClient1PermissionDenied(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "client.crt", "client.key")
	
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas2",
	)

	deleteRequest := taskspb.DeleteTaskRequest{Id: 2}
	_, err := client.DeleteTask(ctx, &deleteRequest)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected error: %v, got %v", codes.PermissionDenied, status.Code(err))
	}
}

func TestMTLSClient2DeleteTaskSuccess(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "client2.crt", "client2.key")
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas2",
	)

	deleteTaskReq := taskspb.DeleteTaskRequest{Id: 2}
	deletedTask, err := client.DeleteTask(ctx, &deleteTaskReq)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if deletedTask.Id != deleteTaskReq.Id {
		t.Errorf("expected deleted task id: %d, got: %d", deleteTaskReq.Id, deletedTask.Id)
	}
	
	getTaskReq := taskspb.GetTaskRequest{Id: 2}
	_, err = client.GetTask(ctx, &getTaskReq)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected error: %v, got: %v", codes.NotFound, status.Code(err))
	}
}

func TestMTLSClient2InvalidToken(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "client2.crt", "client2.key")
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas",
	)

	deleteTaskReq := taskspb.DeleteTaskRequest{Id: 2}
	_, err := client.DeleteTask(ctx, &deleteTaskReq)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected error: %v, got: %v", codes.Unauthenticated, status.Code(err))
	}

	ctxCorrect, cancelSecond := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelSecond()

	ctxCorrect = metadata.AppendToOutgoingContext(
		ctxCorrect,
		"authorization", "sfadfsf3423fdas2",
	)

	deletedTask, err := client.DeleteTask(ctxCorrect, &deleteTaskReq)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if deletedTask.Id != deleteTaskReq.Id {
		t.Errorf("expected deleted task id: %d, got: %d", deleteTaskReq.Id, deletedTask.Id)
	}
}

func TestMTLSWithoutClientCertificate(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "", "")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas2",
	)

	deleteTaskReq := taskspb.DeleteTaskRequest{Id: 2}
	_, err := client.DeleteTask(ctx, &deleteTaskReq)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected %v, got %v", codes.Unavailable, status.Code(err))
	}
}

func TestMTLSClient2WatchTasksSuccess(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "client2.crt", "client2.key")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas2",
	)
	
	stream, err := client.WatchTasks(ctx, &taskspb.WatchTaskRequest{})
	if err != nil {
		t.Fatalf("expected nil error while watch tasks, got: %v", err)
	}
	
	count := 0

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		} 

		if err != nil {
			t.Fatalf("receive task from stream: %v", err)
		}

		count++
	}

	if count != 3 {
		t.Fatalf("expected 3 tasks, got: %d", count)
	}
}

func TestMTLSClient3PermissionDenied(t *testing.T) {
	serverAddr := startTestServer(t)
	client := newTestClient(t, serverAddr, "client3.crt", "client3.key")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization", "sfadfsf3423fdas2",
	)
	
	stream, err := client.WatchTasks(ctx, &taskspb.WatchTaskRequest{})
	if err != nil { 
		t.Fatalf("open stream: %v", err)
	}
	
	_, err = stream.Recv()
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected %v, got: %v", codes.PermissionDenied, err)
	}
}
