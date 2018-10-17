package handlers

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/christianalexander/kvdb/stores"
	"github.com/gorilla/mux"
)

func GetGetHandler(store stores.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["Key"]

		v, err := store.Get(r.Context(), key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get '%s': %v", key, err), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(v))
	})
}

func GetSetHandler(store stores.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["Key"]

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
			return
		}

		err = store.Set(r.Context(), key, string(body))
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to set value: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Add("Location", fmt.Sprintf("/%s", key))
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("OK"))
	})
}

func GetDeleteHandler(store stores.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["Key"]

		err := store.Delete(r.Context(), key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete value: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}
