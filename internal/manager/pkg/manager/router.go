package server

import (
	"github.com/go-chi/chi/v5"
)

func (h *InstanceManager) Serve() *chi.Mux {
	r := chi.NewRouter()
	r.Get("/livez", h.HandleLivez)
	r.Get("/readyz", h.HandleReadyz)

	r.Route("/v1", func(r chi.Router) {
		r.Route("/mongo", func(r chi.Router) {
			// Routes that require 'get' permission on MongoDB resources.
			r.Group(func(r chi.Router) {
				r.Use(h.requirePermission("get", "db.mrhachi.dev", "mongodbs"))
				r.Post("/authenticate", h.HandleAuthenticate)
				r.Get("/topology", h.HandleGetTopology)
				r.Get("/config", h.HandleGetConfig)
			})
			r.Group(func(r chi.Router) {
				r.Use(h.requirePermission("delete", "db.mrhachi.dev", "mongodbs"))
				r.Delete("/members/{podName}", h.HandleRemoveMember)
			})

			// Routes that require 'create' permission on 'mongodbs' in 'db.mrhachi.dev'
			r.Group(func(r chi.Router) {
				r.Use(h.requirePermission("create", "db.mrhachi.dev", "mongodbs"))
				r.Post("/initiate", h.HandleInitiate)
				r.Post("/admin", h.HandleAdmin)
			})
		})

		r.Route("/manager", func(r chi.Router) {
			r.Use(h.requireOperator())
			r.Get("/leadership", h.HandleGetLeadership)
			r.Post("/leadership/transfer", h.HandleTransferLeadership)
		})
	})
	return r
}
