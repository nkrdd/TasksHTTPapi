package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

func TestMTLSClient1PermissionDenied(t *testing.T) {
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
	
	clientCert, err := tls.LoadX509KeyPair(
		filepath.Join(certDir, "client.crt"),
		filepath.Join(certDir, "client.key"),
	)
	if err != nil {
		t.Fatalf("load client certificate: %v", err)
	}

	caPEM, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		t.Fatalf("read ca certificate: %v", err)
	}

	certCAPool := x509.NewCertPool()

	ok := certCAPool.AppendCertsFromPEM(caPEM)
	if !ok {
		t.Fatalf("append certs from pem cause with error")
	}

	tlsConfig := tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs: certCAPool,
		ServerName: "localhost",
	}	

	tcClient := credentials.NewTLS(&tlsConfig)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(tcClient),)
	if err != nil {
		t.Fatalf("create new client connection: %v", err)
	}

	defer conn.Close()

	tsc := taskspb.NewTaskServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	
	md := metadata.New(map[string]string{
		"authorization" : "sfadfsf3423fdas2",
		"request-id" : "req-001",
	})

	ctx = metadata.NewOutgoingContext(ctx, md)
	
	deleteRequest := taskspb.DeleteTaskRequest{Id: 2}
	_, err = tsc.DeleteTask(ctx, &deleteRequest)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}
}
