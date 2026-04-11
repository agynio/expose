package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	exposev1 "github.com/agynio/expose/.gen/go/agynio/api/expose/v1"
	notificationsv1 "github.com/agynio/expose/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/expose/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/expose/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/expose/internal/config"
	"github.com/agynio/expose/internal/db"
	"github.com/agynio/expose/internal/reconciler"
	"github.com/agynio/expose/internal/server"
	"github.com/agynio/expose/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("expose-service: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	zitiConn, err := grpc.NewClient(cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to ziti management: %w", err)
	}
	defer zitiConn.Close()

	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to runners: %w", err)
	}
	defer runnersConn.Close()

	notificationsConn, err := grpc.NewClient(cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to notifications: %w", err)
	}
	defer notificationsConn.Close()

	storeClient := store.New(pool)
	zitiClient := zitimanagementv1.NewZitiManagementServiceClient(zitiConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)

	grpcServer := grpc.NewServer()
	exposev1.RegisterExposeServiceServer(grpcServer, server.New(storeClient, zitiClient, runnersClient))

	lis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddress, err)
	}

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	go reconciler.New(storeClient, zitiClient, runnersClient, notificationsClient, cfg.ReconciliationInterval).Run(ctx)

	log.Printf("ExposeService listening on %s", cfg.GRPCAddress)

	if err := grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
