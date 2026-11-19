package api

import (
	"net/http"
	"os"
	"strconv"
	"sync"
)

// admissionController reserves capacity for field clients under overload (P5.5).
type admissionController struct {
	mu         sync.Mutex
	inFlight   int
	maxTotal   int
	maxMedia   int
	mediaCount int
}

func newAdmissionController() *admissionController {
	total := envPositiveInt("BNPB_API_MAX_IN_FLIGHT", 64)
	media := envPositiveInt("BNPB_API_MEDIA_MAX_IN_FLIGHT", 16)
	if media >= total {
		media = max(1, total/4)
	}
	return &admissionController{maxTotal: total, maxMedia: media}
}

func (controller *admissionController) admit(role string) bool {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if role == roleField || role == roleOps {
		controller.inFlight++
		return true
	}
	if role == roleMedia && (controller.inFlight >= controller.maxTotal || controller.mediaCount >= controller.maxMedia) {
		return false
	}
	controller.inFlight++
	if role == roleMedia {
		controller.mediaCount++
	}
	return true
}

func (controller *admissionController) release(role string) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.inFlight > 0 {
		controller.inFlight--
	}
	if role == roleMedia && controller.mediaCount > 0 {
		controller.mediaCount--
	}
}

func (service *publicService) withAdmission(next func(http.ResponseWriter, *http.Request, principal)) func(http.ResponseWriter, *http.Request, principal) {
	return func(w http.ResponseWriter, request *http.Request, identity principal) {
		if service.admission == nil {
			next(w, request, identity)
			return
		}
		if !service.admission.admit(identity.role) {
			writePublicError(w, request, http.StatusTooManyRequests, "capacity_exceeded",
				"system is at capacity; retry later")
			return
		}
		defer service.admission.release(identity.role)
		next(w, request, identity)
	}
}

func envPositiveInt(name string, fallback int) int {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return fallback
	}
	return value
}