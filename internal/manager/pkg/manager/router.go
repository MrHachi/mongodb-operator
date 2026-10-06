package server

import (
	"github.com/go-chi/chi/v5"
)

func (h *InstanceManager) Serve() *chi.Mux {
	r := chi.NewRouter()
	r.Get("/livez", h.HandleLivez)
	r.Get("/readyz", h.HandleReadyz)

	r.Route("/v1", func(r chi.Router) {
		// Routes that require 'get' permission on MongoDB resources.
		r.Group(func(r chi.Router) {
			r.Use(h.RequirePermission("get", "db.mrhachi.dev", "mongodbs"))
			r.Post("/authenticate", h.HandleAuthenticate)
			r.Get("/topology", h.HandleGetTopology)
		})

		// Routes that require 'create' permission on 'mongodbs' in 'db.mrhachi.dev'
		r.Group(func(r chi.Router) {
			r.Use(h.RequirePermission("create", "db.mrhachi.dev", "mongodbs"))
			r.Post("/initiate", h.HandleInitiate)
			r.Post("/admin", h.HandleAdmin)
		})
	})

	return r
}
