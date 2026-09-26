package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	taskspb "tasks/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func main() {
	clientCertificate, err := tls.LoadX509KeyPair("certs/client.crt", "certs/client.key")
	if err != nil {
		log.Printf("load client certificate: %v\n", err)
		return
	}
	
	caPEM, err := os.ReadFile("certs/ca.crt")
	if err != nil {
		log.Printf("read ca certificate: %v", err)
		return
	}

	certCAPool := x509.NewCertPool()

	ok := certCAPool.AppendCertsFromPEM(caPEM)
	if !ok {
		log.Printf("append certs from PEM to certs pool cause with error")
		return
	}

	tlsConfig := tls.Config{
		Certificates: []tls.Certificate{clientCertificate},
		RootCAs: certCAPool,
		ServerName: "localhost",
	}	

	tc := credentials.NewTLS(&tlsConfig)

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(tc),)
	if err != nil {
		log.Printf("create client for server: %v", err)
		return
	}

	hc := grpc_health_v1.NewHealthClient(conn)
	defer conn.Close()

	tsc := taskspb.NewTaskServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	healthResponse, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	if err != nil {
		log.Printf("health check: %v\n", err)
		return
	}

	servingStatus := healthResponse.GetStatus()
	log.Println(servingStatus)
	

	md := metadata.New(map[string]string{
		"authorization" : "sfadfsf3423fdas2",
		"request-id" : "req-001",
	})

	ctx = metadata.NewOutgoingContext(ctx, md)

	localTasks := []*taskspb.Task{
		&taskspb.Task{Id: 1, Title: "buy milk", Done: false},
		&taskspb.Task{Id: 2, Title: "buy eggs", Done: false}, 
		&taskspb.Task{Id: 3, Title: "buy coke", Done: false},
	}

	newStream2, err := tsc.SyncTasks(ctx)
	if err != nil {
		log.Printf("sync tasks: %v", err)
		return
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, task := range localTasks {
			err := newStream2.Send(task)
			if err != nil {
				log.Printf("send task: %v", err)
				cancel()
				return
			}
		}
		err = newStream2.CloseSend()
		if err != nil {
			log.Printf("close send in stream: %v", err)
			cancel()
			return
		}
	}()

	wg.Add(1)
	go func () {
		defer wg.Done()
		for {
			taskResult, err := newStream2.Recv()
			if err != nil {
				if err == io.EOF {
					log.Printf("all responses has been handled")
					return
				}
				log.Printf("receive taskResult: %v", err)
				cancel()
				return
			}

			log.Printf("get taskResult: %d %s", taskResult.Id, taskResult.Message)
		}
	}()

	wg.Wait()
	
	deleteRequest := taskspb.DeleteTaskRequest{Id: 2}
	deletedTask, err := tsc.DeleteTask(ctx, &deleteRequest)	
	if err != nil {
		log.Printf("delete task: %v\n", err)
		return
	}
	
	fmt.Printf("deleted task: %s %d\n", deletedTask.Title, deletedTask.Id)

	getTaskRequest := taskspb.GetTaskRequest{Id: 1} 
	task, err := tsc.GetTask(ctx, &getTaskRequest)
	if err != nil {
		log.Printf("get task: %v\n", err)
		return
	}

	fmt.Println(task)
}
