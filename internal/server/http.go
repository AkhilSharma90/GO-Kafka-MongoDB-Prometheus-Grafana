package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/AleksK1NG/products-microservice/docs"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	echoSwagger "github.com/swaggo/echo-swagger"
)

// runHTTPServer starts the HTTP server. This call will BLOCK until the server is closed or errors out.
func (s *server) runHTTPServer() error {
	// Basic routes
	s.echo.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Server running on HTTP")
	})

	s.echo.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "Test route working!")
	})

	s.echo.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "Ok")
	})

	s.echo.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	// Configure Swagger *for HTTP* (no HTTPS).
	docs.SwaggerInfo.Title = "Products microservice"
	docs.SwaggerInfo.Description = "Products REST API microservice."
	docs.SwaggerInfo.Version = "1.0"
	docs.SwaggerInfo.BasePath = "/api/v1"
	docs.SwaggerInfo.Host = "localhost:5007"
	docs.SwaggerInfo.Schemes = []string{"http"}

	s.echo.GET("/swagger/*", echoSwagger.WrapHandler)
	s.echo.GET("/swagger", func(c echo.Context) error {
		// If user goes to /swagger, redirect them to /swagger/index.html
		return c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})

	// Common middleware (no SSL/HTTPS redirection):
	s.echo.Use(middleware.Logger())
	s.echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{
			echo.HeaderOrigin,
			echo.HeaderContentType,
			echo.HeaderAccept,
			echo.HeaderXRequestID,
			csrfTokenHeader,
		},
	}))
	s.echo.Use(middleware.Recover())
	s.echo.Use(middleware.RequestID())
	s.echo.Use(middleware.GzipWithConfig(middleware.GzipConfig{
		Level: gzipLevel,
		Skipper: func(c echo.Context) bool {
			return strings.Contains(c.Request().URL.Path, "swagger")
		},
	}))
	s.echo.Use(middleware.BodyLimit(bodyLimit))

	addr := s.cfg.Http.Port
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	s.echo.Server.ReadTimeout = time.Second * s.cfg.Http.ReadTimeout
	s.echo.Server.WriteTimeout = time.Second * s.cfg.Http.WriteTimeout
	s.echo.Server.MaxHeaderBytes = maxHeaderBytes

	s.log.Infof("Starting HTTP server on %s", addr)
	// BLOCKING call (will not return until the server is closed or fails).
	return s.echo.Start(addr)
}
