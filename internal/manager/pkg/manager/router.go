package server

import (
	"github.com/go-chi/chi/v5"
)

func (h *InstanceManager) Serve() *chi.Mux {
	r := chi.NewRouter()

	r.Route("/v1", func(r chi.Router) {
		// Routes that require 'create' permission on 'mongodbs' in 'db.mrhachi.dev'
		r.Group(func(r chi.Router) {
			r.Use(h.RequirePermission("create", "db.mrhachi.dev", "mongodbs"))
			r.Post("/initiate", h.HandleInitiate)
			r.Post("/admin", h.HandleAdmin)
		})

		// Routes that require 'get' permission on 'mongodbs' in 'db.mongodb.dev'
		r.Group(func(r chi.Router) {
			r.Use(h.RequirePermission("get", "db.mongodb.dev", "mongodbs"))
			r.Get("/topology", h.HandleGetTopology)
		})
	})

	return r
}
