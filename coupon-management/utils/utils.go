package utils

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
)

var fluentTag string

type APIRequest struct {
	Cid            int    `json:"cid"`
	Coupon_list_id int    `json:"coupon_list_id"`
	Quantity       int    `json:"quantity"`
	Request_id     string `json:"request_id"`
}
type APIResponse struct {
	ResponseCode   int32    `json:"response_code"`
	Status         string   `json:"status"`
	Message        string   `json:"message"`
	Coupons        []string `json:"coupons"`
	Expiry         string   `json:"coupon_expiry"`
	Coupon_list_id int      `json:"coupon_list_id"`
}
type BulkApiResponse struct {
	ResponseCode int32  `json:"response_code"`
	Message      string `json:"message"`
	//,omitempty
}

type Mysqldata struct {
	Coupon_code   []string `json:"coupon_code"`
	Coupon_expiry string   `json:"coupon_expiry"`
}

func initFluent() {
	var err error

	fluentHost, fluentHostExists := os.LookupEnv("FLUENTHOST")
	fluentTag := os.Getenv("FLUENTTAG")
	fmt.Println(fluentTag)

	if fluentHostExists {
		//fmt.Printf("Conecting to fluent : %s ", os.Getenv("FLUENTHOST"))
		// Flogger, err = fluent.New(fluent.Config{FluentPort: 24224, FluentHost: os.Getenv("FLUENTHOST")})
		Flogger, err = fluent.New(fluent.Config{FluentPort: 24224, FluentHost: fluentHost, WriteTimeout: 500 * time.Millisecond, Timeout: 1 * time.Second, Async: true})
		if err != nil {
			fmt.Printf("Could not connect to Fluent at %s Error : %v", os.Getenv("FLUENTHOST"), err)
		}
		fmt.Println("fluent connected", Flogger)
	}
}

// InfoLog (msg string)  Logs the string to Fluent server
func InfoLog(requestID string, msg string) {
	log(requestID, msg, "INFO")
}

// ErrorLog : logging
func ErrorLog(requestID string, msg string) {
	log(requestID, msg, "ERROR")
}

// WarningLog : logging
func WarningLog(requestID string, msg string) {
	log(requestID, msg, "WARN")
}

func log(requestID string, msg string, logLevel string) {
	err := Flogger.Post(fluentTag, map[string]string{fmt.Sprintf("%s - <%s>", logLevel, requestID): msg})
	if err != nil {
		fmt.Printf("%s - <%s> Could not log to fluent %s", logLevel, requestID, err.Error())
	}

}

func Validate_file_get_valid_invalid_records(fileContent string) (string, string) {
	validfileContent := fileContent
	invalidfileContent := ""
	return validfileContent, invalidfileContent
}

func Get_tableName_from_file(request_id, listid string, clientid string) string {
	fmt.Println(request_id, fmt.Sprintf("%s%s_%s", os.Getenv("Table_substr"), clientid, listid))
	return fmt.Sprintf("%s%s_%s", os.Getenv("Table_substr"), clientid, listid)
}

func Coupon_data_formatting(valid_records string) []string {
	Coupons_arr := strings.Split(valid_records, "\n")
	var final_coupon []string
	// Extract the header from the first line
	for _, coupon := range Coupons_arr {
		if coupon != "" {
			final_coupon = append(final_coupon, coupon)
		}
	}
	return final_coupon
}

func RemoveBadCharacters(input string) string {
	re := regexp.MustCompile(os.Getenv("BadChar"))

	cleaned := re.ReplaceAllString(input, "")
	cleaned = strings.Trim(cleaned, "\n")

	return cleaned
}
