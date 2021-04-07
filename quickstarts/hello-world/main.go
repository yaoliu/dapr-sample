package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/http"
	"log"
	"net/http"
)

const (
	stateUrl       = "http://localhost:3500/v1.0/state/statestore"
	stateStoreName = "statestore"
)

func main() {
	s := daprd.NewService(":60000")
	if err := s.AddServiceInvocationHandler("/neworder", newOrderHandler); err != nil {
		log.Fatalf("error adding invocation handler: %v", err)
	}

	if err := s.AddServiceInvocationHandler("/order", orderHandler); err != nil {
		log.Fatalf("error adding invocation handler: %v", err)
	}

	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("error: %v", err)
	}
}

func newOrderHandler(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
	if in == nil {
		err = errors.New("invocation parameter required")
		return
	}
	log.Printf("new order - ContentType:%s, Verb:%s, QueryString:%s, %+v", in.ContentType, in.Verb, in.QueryString, string(in.Data))

	s := map[string]map[string]string{}
	_ = json.Unmarshal(in.Data, &s)
	orderId := s["data"]["orderId"]
	log.Printf("Got a new order! Order ID: %s", orderId)

	daprClient, err := client.NewClient()
	if err != nil {
		log.Fatalf("new client error: %v", err)
		return
	}
	if err = daprClient.SaveState(ctx, stateStoreName, "order", in.Data); err != nil {
		log.Fatalf("save state error: %v", err)
		return
	}
	out = &common.Content{
		Data:        nil,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}
	return
}

func orderHandler(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
	if in == nil {
		err = errors.New("invocation parameter required")
		return
	}
	log.Printf("new order - ContentType:%s, Verb:%s, QueryString:%s, %+v", in.ContentType, in.Verb, in.QueryString, string(in.Data))
	daprClient, err := client.NewClient()
	if err != nil {
		log.Fatalf("new client error: %v", err)
		return
	}
	item, err := daprClient.GetState(ctx, stateStoreName, "order")
	if err != nil {
		log.Fatalf("get state error: %v", err)
		return
	}
	out = &common.Content{
		Data:        item.Value,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}
	return
}
