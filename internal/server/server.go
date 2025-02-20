package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	// Core dependencies
	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	// gRPC middleware
	grpcrecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"

	// Echo
	"github.com/labstack/echo/v4"

	// Internal packages
	"github.com/AleksK1NG/products-microservice/config"
	"github.com/AleksK1NG/products-microservice/internal/interceptors"
	"github.com/AleksK1NG/products-microservice/internal/middlewares"

	// IMPORTANT: rename your local “internal/product/delivery/grpc” package to avoid name collision:
	productGrpc "github.com/AleksK1NG/products-microservice/internal/product/delivery/grpc"
	productsHttpV1 "github.com/AleksK1NG/products-microservice/internal/product/delivery/http/v1"
	"github.com/AleksK1NG/products-microservice/internal/product/delivery/kafka"
	"github.com/AleksK1NG/products-microservice/internal/product/repository"
	"github.com/AleksK1NG/products-microservice/internal/product/usecase"
	"github.com/AleksK1NG/products-microservice/pkg/logger"
	productsService "github.com/AleksK1NG/products-microservice/proto/product"
)

const (
	maxHeaderBytes  = 1 << 20
	gzipLevel       = 5
	stackSize       = 1 << 10
	csrfTokenHeader = "X-CSRF-Token"
	bodyLimit       = "2M"

	kafkaGroupID = "products_group"
)

type server struct {
	log     logger.Logger
	cfg     *config.Config
	tracer  opentracing.Tracer
	mongoDB *mongo.Client
	echo    *echo.Echo
	redis   *redis.Client
}

// NewServer constructs our main server object.
func NewServer(
	log logger.Logger,
	cfg *config.Config,
	tracer opentracing.Tracer,
	mongoDB *mongo.Client,
	redis *redis.Client,
) *server {
	return &server{
		log:     log,
		cfg:     cfg,
		tracer:  tracer,
		mongoDB: mongoDB,
		echo:    echo.New(),
		redis:   redis,
	}
}

// Run starts up everything (gRPC, Kafka consumers, and the HTTP server).
func (s *server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Validator for incoming requests.
	validate := validator.New()

	// Kafka Producer.
	productsProducer := kafka.NewProductsProducer(s.log, s.cfg)
	productsProducer.Run()
	defer productsProducer.Close()

	// Setup Repos & UseCase.
	productMongoRepo := repository.NewProductMongoRepo(s.mongoDB)
	productRedisRepo := repository.NewProductRedisRepository(s.redis)
	productUC := usecase.NewProductUC(productMongoRepo, productRedisRepo, s.log, productsProducer)

	// Interceptors & Middlewares.
	im := interceptors.NewInterceptorManager(s.log, s.cfg)
	mw := middlewares.NewMiddlewareManager(s.log, s.cfg)

	// gRPC Server setup.
	grpcAddr := s.cfg.Server.Port
	if !strings.HasPrefix(grpcAddr, ":") {
		grpcAddr = ":" + grpcAddr
	}
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return errors.Wrap(err, "net.Listen")
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: s.cfg.Server.MaxConnectionIdle * time.Minute,
			Timeout:           s.cfg.Server.Timeout * time.Second,
			MaxConnectionAge:  s.cfg.Server.MaxConnectionAge * time.Minute,
			Time:              s.cfg.Server.Timeout * time.Minute,
		}),
		grpc.ChainUnaryInterceptor(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_opentracing.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpcrecovery.UnaryServerInterceptor(),
			im.Logger,
		),
	)

	// Register our product service implementation with gRPC.
	prodSvc := productGrpc.NewProductService(s.log, productUC, validate)
	productsService.RegisterProductsServiceServer(grpcServer, prodSvc)
	grpc_prometheus.Register(grpcServer)

	// HTTP routes for /api/v1/products (this is separate from the top-level routes in http.go).
	v1 := s.echo.Group("/api/v1")
	v1.Use(mw.Metrics)

	productHandlers := productsHttpV1.NewProductHandlers(s.log, productUC, validate, v1.Group("/products"), mw)
	productHandlers.MapRoutes()

	// Kafka Consumer Group.
	productsCG := kafka.NewProductsConsumerGroup(
		s.cfg.Kafka.Brokers,
		kafkaGroupID,
		s.log,
		s.cfg,
		productUC,
		validate,
	)

	// Start gRPC in background.
	go func() {
		s.log.Infof("gRPC server listening on %s", grpcAddr)
		if err := grpcServer.Serve(listener); err != nil {
			s.log.Errorf("gRPC server error: %v", err)
			cancel()
		}
	}()

	// Start Kafka consumers in background.
	go productsCG.RunConsumers(ctx, cancel)

	// Start a separate metrics server in background (optional).
	go func() {
		m := echo.New()
		m.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
		s.log.Infof("Metrics server on %s", s.cfg.Metrics.Port)
		if err := m.Start(s.cfg.Metrics.Port); err != nil {
			s.log.Errorf("Metrics server: %v", err)
			cancel()
		}
	}()

	// Start the main HTTP server (defined in http.go), in background.
	go func() {
		if errHTTP := s.runHTTPServer(); errHTTP != nil {
			s.log.Errorf("HTTP server error: %v", errHTTP)
			cancel()
		}
	}()

	// Wait for OS signal or context cancellation.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		s.log.Warnf("Received signal: %v, shutting down...", sig)
	case <-ctx.Done():
		s.log.Warnf("Context canceled, shutting down servers...")
	}

	// Graceful shutdown attempts:
	grpcServer.GracefulStop()
	if err := s.echo.Shutdown(context.Background()); err != nil {
		s.log.Errorf("Error on shutting down HTTP server: %v", err)
	}
	_ = listener.Close()

	s.log.Info("Server exited properly")
	return nil
}
