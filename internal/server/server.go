package server

import (
	"html/template"
	"net/http"
	"order_service/internal/cache"

	"github.com/labstack/echo/v4"
)

type Server struct {
	e   *echo.Echo
	cch *cache.Cache
}

func New(cch *cache.Cache) *Server {
	e := echo.New()

	renderer := &TemplateRenderer{
		templates: template.Must(template.ParseGlob("templates/index.html")),
	}

	e.Renderer = renderer

	return &Server{
		e:   e,
		cch: cch,
	}
}

func (s *Server) initHandlers() {
	s.e.GET("/:id", s.viewData)
}

func (s *Server) Start() error {
	s.initHandlers()
	return s.e.Start(":8080")
}

func (s *Server) viewData(c echo.Context) error {
	id := c.Param("id")

	orderInfo, _ := s.cch.Get(id)

	return c.Render(http.StatusOK, "index.html", orderInfo)

}
