package server

import (
	"github.com/go-chi/chi/v5"
)

func (h *InstanceManager) Serve() *chi.Mux {
	r := chi.NewRouter()

	r.Route("/v1", func(r chi.Router) {
		r.Use(h.Authenticate)
		r.Post("/initiate", h.HandleInitiate)
		r.Post("/admin", h.HandleAdmin)
	})

	return r
}
