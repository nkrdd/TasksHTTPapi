package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	taskspb "tasks/proto"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

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
