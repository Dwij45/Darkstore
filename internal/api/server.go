// Package api exposes the dark-store inventory over HTTP.
//
//	GET  /healthz
//	GET  /stock
//	POST /checkout              (requires Idempotency-Key)
//	GET  /orders/{id}
//	POST /orders/{id}/confirm   payment succeeded: sell the held stock
//	POST /orders/{id}/cancel    give the held stock back
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"darkstore/internal/inventory"
)

const maxBodyBytes = 64 << 10

// Dispatcher receives orders once payment is confirmed. Nil means confirmed
// orders are not handed to anyone (used before the worker pool exists).
type Dispatcher interface {
	Submit(ctx context.Context, orderID string) error
}

type Server struct {
	inv      *inventory.Store
	dispatch Dispatcher
	idem     *idempotencyCache
	mux      *http.ServeMux
}

func New(inv *inventory.Store, dispatch Dispatcher) *Server {
	s := &Server{
		inv:      inv,
		dispatch: dispatch,
		idem:     newIdempotencyCache(),
		mux:      http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /stock", s.handleStock)
	s.mux.HandleFunc("POST /checkout", s.handleCheckout)
	s.mux.HandleFunc("GET /orders/{id}", s.handleGetOrder)
	s.mux.HandleFunc("POST /orders/{id}/confirm", s.handleConfirm)
	s.mux.HandleFunc("POST /orders/{id}/cancel", s.handleCancel)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type checkoutRequest struct {
	UserID string           `json:"user_id"`
	Items  []inventory.Item `json:"items"`
}

type orderResponse struct {
	OrderID   string           `json:"order_id"`
	Status    inventory.Status `json:"status"`
	Items     []inventory.Item `json:"items"`
	ExpiresAt time.Time        `json:"expires_at"`
}

type errorResponse struct {
	Error     string `json:"error"`
	SKU       string `json:"sku,omitempty"`
	Available *int   `json:"available,omitempty"`
}

func toOrder(r inventory.Reservation) orderResponse {
	return orderResponse{OrderID: r.ID, Status: r.Status, Items: r.Items, ExpiresAt: r.ExpiresAt}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStock(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.inv.Stock())
}

func (s *Server) handleCheckout(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "Idempotency-Key header is required"})
		return
	}
	var req checkoutRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body: " + err.Error()})
		return
	}
	if req.UserID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "user_id is required"})
		return
	}

	// Fingerprint the decoded request, not the raw bytes, so whitespace or
	// field order in a retry does not count as a different request.
	canonical, _ := json.Marshal(req)
	fp := sha256.Sum256(canonical)

	// Keys are scoped per user so two users cannot collide on the same key.
	e, owner, err := s.idem.begin(req.UserID+":"+key, fp)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, errorResponse{Error: err.Error()})
		return
	}
	if !owner {
		select {
		case <-e.done:
			w.Header().Set("Idempotent-Replayed", "true")
			writeRaw(w, e.status, e.body)
		case <-r.Context().Done():
		}
		return
	}

	// Every outcome is stored, including 409s: a retry of a request that found
	// no stock gets the same answer rather than a surprise success later.
	status, payload := s.checkout(req)
	body, _ := json.Marshal(payload)
	s.idem.finish(e, status, body)
	writeRaw(w, status, body)
}

func (s *Server) checkout(req checkoutRequest) (int, any) {
	res, err := s.inv.Reserve(req.Items)
	var oos *inventory.OutOfStockError
	switch {
	case err == nil:
		return http.StatusCreated, toOrder(res)
	case errors.As(err, &oos):
		return http.StatusConflict, errorResponse{Error: "insufficient stock", SKU: oos.SKU, Available: &oos.Available}
	case errors.Is(err, inventory.ErrUnknownSKU),
		errors.Is(err, inventory.ErrInvalidQty),
		errors.Is(err, inventory.ErrEmptyOrder):
		return http.StatusUnprocessableEntity, errorResponse{Error: err.Error()}
	default:
		return http.StatusInternalServerError, errorResponse{Error: "internal error"}
	}
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	res, err := s.inv.Get(r.PathValue("id"))
	if err != nil {
		writeOrderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrder(res))
}

func (s *Server) handleConfirm(w http.ResponseWriter, r *http.Request) {
	res, err := s.inv.Confirm(r.PathValue("id"))
	if err != nil {
		writeOrderError(w, err)
		return
	}
	if s.dispatch != nil {
		if err := s.dispatch.Submit(r.Context(), res.ID); err != nil {
			// Payment is confirmed but no worker took the order. Without an
			// outbox this order stays confirmed and unfulfilled; see README.
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "order confirmed but fulfilment queue is unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, toOrder(res))
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	res, err := s.inv.Release(r.PathValue("id"))
	if err != nil {
		writeOrderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrder(res))
}

func writeOrderError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, inventory.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{Error: err.Error()})
	case errors.Is(err, inventory.ErrExpired):
		writeJSON(w, http.StatusGone, errorResponse{Error: err.Error()})
	case errors.Is(err, inventory.ErrNotPending):
		writeJSON(w, http.StatusConflict, errorResponse{Error: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeRaw(w, status, body)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
	w.Write([]byte("\n"))
}
