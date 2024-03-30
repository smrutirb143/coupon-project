package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"

	couponloader "coupon/couponCacheLoader/coupon-cache-loader"
	fetch "coupon/couponFetchApi/coupon-fetch-api"
	login "coupon/logconfig"
	report "coupon/reporting-service/reporting-service"
)

var Logger *login.Logger
var (
	UUID string
)

func Checkhealth(w http.ResponseWriter, r *http.Request) {
	apiEndpoints := map[string]string{
		"API 1": "http://localhost:4343",
		"API 2": "http://localhost:4344",
		"API 3": "http://localhost:4345",
	}

	var (
		mu           sync.Mutex
		healthStatus []string
		wg           sync.WaitGroup
	)

	for name, endpoint := range apiEndpoints {
		wg.Add(1)
		go func(name, endpoint string) {
			defer wg.Done()
			if checkAPIHealth(endpoint) {
				mu.Lock()
				healthStatus = append(healthStatus, fmt.Sprintf("%s is healthy", name))
				mu.Unlock()
			} else {
				mu.Lock()
				healthStatus = append(healthStatus, fmt.Sprintf("%s is unhealthy", name))
				mu.Unlock()
			}
		}(name, endpoint)
	}

	wg.Wait()

	// Write the health status as JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(healthStatus)
}

func checkAPIHealth(endpoint string) bool {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	_, err := client.Get(endpoint + "/health")
	return err == nil
}

func CouponApi() {
	router := mux.NewRouter()
	router.HandleFunc("/couponCacheLoad", couponloader.CouponCacheLoad).Methods("POST")
	router.HandleFunc("/coupons/request", fetch.CouponFetchredis).Methods("POST")
	router.HandleFunc("/coupons/stats", report.ReportApi).Methods("POST")
	router.HandleFunc("/healthcheck", Checkhealth).Methods("GET")
	//router.HandleFunc("/checkAPIHealth", health.Checkhealth).Methods("GET")
	//router.HandleFunc("/couponCacheReload", FetchCouponmysql).Methods("POST")
	go startServer(":4343", router)
	go startServer(":4344", router)
	go startServer(":4345", router)
	go startServer(":4346", router)

	// Keep the main goroutine running
	select {}
}

func startServer(addr string, router http.Handler) {
	// Start an HTTP server on the specified address with the provided router
	fmt.Printf("Server listening on %s\n", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		fmt.Printf("Failed to start server on %s: %v\n", addr, err)
	}
}

func main() {
	CouponApi()

}
