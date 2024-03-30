package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/gomodule/redigo/redis"

	_ "github.com/go-sql-driver/mysql"
)

var (
	s3Client *s3.S3
	Flogger  *fluent.Fluent
	Pool     *redis.Pool
	Conn     *redis.Conn
)

var Redis_pool *redis.Pool

func init() {

	//initmysql()
	initFluent()
	inits3()

}

func inits3() {

	region, is_region_exist := os.LookupEnv("region")
	if !is_region_exist {
		InfoLog("WARNING", "Kindly set up the os variable for region")

	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		fmt.Printf("Failed to create session: %s", err)
	}
	s3Client = s3.New(sess)
}

var (
	Mysqldb     *sql.DB
	dbMutex     sync.Mutex
	dbInitiated bool
)

func getDBConnection() (*sql.DB, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	dbHost := os.Getenv("RDS_HOSTNAME")
	dbName := os.Getenv("RDS_DBNAME")
	dbUser := os.Getenv("RDS_USERNAME")
	dbPassword := os.Getenv("RDS_PASSWORD")
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPassword, dbHost, dbName)

	if !dbInitiated {
		var err error
		Mysqldb, err = sql.Open("mysql", connectionString)
		if err != nil {
			return nil, fmt.Errorf("error opening database connection: %v", err)
		}
		dbInitiated = true
	}

	return Mysqldb, nil
}

func getCouponStr(key string, cid int, listid int) string {

	tablename := fmt.Sprintf("%s%s_%s", key, strconv.Itoa(cid), strconv.Itoa(listid))
	return tablename
}

func formatStruct(Couponstr []Mysqldata) APIResponse {
	var couponCodes []string
	var final_str APIResponse
	for _, data := range Couponstr {
		couponCodes = append(couponCodes, data.Coupon_code...)
	}
	final_str.Coupons = couponCodes
	final_str.Expiry = Couponstr[0].Coupon_expiry

	return final_str
}
func unmarshalRedisJSON(redisString string) (Mysqldata, error) {
	// Unmarshal the JSON string into the struct
	var mysqlData Mysqldata
	err := json.Unmarshal([]byte(redisString), &mysqlData)
	if err != nil {
		return Mysqldata{}, err
	}
	return mysqlData, nil
}

func updateStatusused(coupons []string, Coupon_table string, status string) bool {
	query := fmt.Sprintf("UPDATE %s SET value = ? WHERE coupon_code = ?", Coupon_table)
	tx, err := Mysqldb.Begin()
	if err != nil {
		return false
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return false
	}
	defer stmt.Close()

	for _, coupon := range coupons {
		_, err = stmt.Exec(status, coupon)
		if err != nil {
			tx.Rollback()
			return false
		}
	}

	err = tx.Commit()
	if err != nil {
		return false
	}
	return true
}

func GetFileFromS3(request_id string, fileName string, direct bool) ([]byte, error) {
	start := time.Now()
	if strings.HasPrefix(fileName, "https://") {
		direct = true
	}

	if !direct {
		bucket := os.Getenv("S3_BUCKET")
		result, err := s3Client.GetObjectWithContext(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fileName),
		})
		if err != nil {
			ErrorLog("Error listing objects:", err.Error())
			return nil, err
		}
		defer result.Body.Close()

		fileContent, err := ioutil.ReadAll(result.Body)
		if err != nil {
			ErrorLog("Error reading S3 file content:", err.Error())
			return nil, err
		}

		InfoLog(request_id, fmt.Sprintf("File fetched from S3:%s", fileName))
		return fileContent, nil
	} else {
		result, err := http.Get(fileName)
		if err != nil {
			ErrorLog("Error making GET request:", err.Error())
			return nil, err
		}
		defer result.Body.Close()

		fileContent, err := ioutil.ReadAll(result.Body)
		if err != nil {
			ErrorLog(request_id, fmt.Sprintf("Error reading response body:%s", err.Error()))
			return nil, err
		}
		duration := time.Since(start)
		InfoLog(request_id, fmt.Sprintf("GetFileFromS3 execution time:%s", duration))

		return fileContent, nil
	}
}

func Create_table(request_id, table_name string) bool {
	start := time.Now()

	createCouponTableSQL := `
        CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
			coupon_code VARCHAR(255) NOT NULL UNIQUE,
			coupon_expiry DATETIME NOT NULL,
			fallback_code VARCHAR(255) NOT NULL,
			token_type VARCHAR(255) DEFAULT 'static',
			tag VARCHAR(255),
			status VARCHAR(255) DEFAULT 'available'
        )

    `
	_, err := Mysqldb.Exec(fmt.Sprintf(createCouponTableSQL, table_name))
	if err != nil {
		ErrorLog(request_id, fmt.Sprintf("Error creating table:%s", err))
		return false
	} else {
		InfoLog(request_id, fmt.Sprintf("The table created sccessfully %s", table_name))
	}
	duration := time.Since(start)
	InfoLog(request_id, fmt.Sprintf("Create_table execution time:%s", duration))
	return true
}

func Bulkinsert(request_id string, Coupons_arr []string, Type string, tag string, fallbackCode string, expired_time string, table_name string) int {
	batchSize, _ := strconv.Atoi(os.Getenv("Batch_size"))
	primary_status := os.Getenv("Primary_status")
	Coupons_arr = Coupons_arr[1:]
	maxIter := (len(Coupons_arr) + batchSize - 1) / batchSize
	var result sql.Result
	sqlStr := fmt.Sprintf("INSERT IGNORE INTO %s (coupon_code, coupon_expiry, fallback_code, token_type, tag, status) VALUES ", table_name)
	var count int
	for i := 0; i < maxIter; i++ {
		start := time.Now()
		end := (i + 1) * batchSize
		if end > len(Coupons_arr) {
			end = len(Coupons_arr)
		}
		batchVals := []interface{}{}
		valueStrings := []string{}
		for _, coupon := range Coupons_arr[i*batchSize : end] {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
			batchVals = append(batchVals, strings.Trim(coupon, " \t\n\r"), expired_time, fallbackCode, Type, tag, primary_status)
		}

		sql := sqlStr + strings.Join(valueStrings, ",") // fmt.Sprintf(" ON DUPLICATE KEY UPDATE coupon_expiry = '%s'", expired_time)
		stmt, err := Mysqldb.Prepare(sql)
		if err != nil {
			ErrorLog(request_id, fmt.Sprintf("Error in making mysql statement %s", err))
			continue
		}
		defer stmt.Close()
		result, err = stmt.Exec(batchVals...)
		if err != nil {
			ErrorLog(request_id, fmt.Sprintf("Error in insert into mysql statement %s", err))
			if err := retryInsert(request_id, sql, batchVals); err != nil {
				continue
			}
		}
		if err != nil {
			return 0
		}
		c, _ := result.RowsAffected()
		count += int(c)

		duration := time.Since(start)
		InfoLog(request_id, fmt.Sprintf("Bulkinsert execution time:%s", duration))
	}
	return count
}

func retryInsert(requestID string, sql string, batchVals []interface{}) error {
	maxRetries, _ := strconv.Atoi(os.Getenv("max_retry"))
	delay := 100 // milliseconds
	for i := 0; i < maxRetries; i++ {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		stmt, err := Mysqldb.Prepare(sql)
		if err != nil {
			ErrorLog(requestID, fmt.Sprintf("Error in making mysql statement for retry %d: %s", i+1, err))
			continue
		}
		defer stmt.Close()
		_, err = stmt.Exec(batchVals...)
		if err == nil {
			// Retry successful
			return nil
		}
		InfoLog(requestID, fmt.Sprintf("Error in insert into mysql statement for retry %d: %s", i+1, err))
		// Increase delay for exponential backoff
		delay *= 2
	}
	return fmt.Errorf("maximum retries reached")
}

func Updatemetainfo(clientid string, listid string, insertedCount int, Type string, expired_time string, fileName string, fallbackCode string, requestid string) bool {
	cid, _ := strconv.Atoi(clientid)
	lid, _ := strconv.Atoi(listid)

	query := fmt.Sprintf("INSERT INTO coupon_store_meta (cid, listID, created_date, updated_date, total_coupon, available_coupon, type,status, expiry, filename, fallback_code) VALUES (%d, %d, NOW(), NOW(), %d, %d, '%s','active', '%s', '%s', '%s') ON DUPLICATE KEY UPDATE total_coupon = total_coupon + %d, available_coupon = available_coupon + %d", cid, lid, insertedCount, insertedCount, Type, expired_time, fileName, fallbackCode, insertedCount, insertedCount)
	fmt.Println(query)
	_, err := Mysqldb.Exec(query)
	if err != nil {
		ErrorLog(requestid, fmt.Sprintf(err.Error()))
		return false
	}
	return true
}

func Client_upload_logs(upload_status string, clientid string, listid string, client_file_name string, failed_file_path string, total_records int, failed_records int, valid_records int, RequestID string) bool {
	cid, _ := strconv.Atoi(clientid)
	lid, _ := strconv.Atoi(listid)
	Mysqldb, err := getDBConnection()
	if err != nil {
		ErrorLog(RequestID, err.Error())
		return false
	}
	sqlQuery := ""
	if upload_status == "processing" {
		sqlQuery = "INSERT IGNORE INTO coupon_upload_logs (upload_status, clientid, listid, client_file_name, failed_file_path, total_records, failed_records, valid_records) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	} else {

		sqlQuery = "UPDATE coupon_upload_logs SET upload_status=?, client_file_name=?, failed_file_path=?, total_records=?, failed_records=?, valid_records=? WHERE clientid=? AND listid=? order by jobid desc limit 1"
	}

	fmt.Println(sqlQuery, upload_status)

	stmt, err := Mysqldb.Prepare(sqlQuery)
	if err != nil {
		ErrorLog(RequestID, err.Error())
		return false
	}

	defer stmt.Close()

	var result sql.Result
	fmt.Println(upload_status, client_file_name, failed_file_path, total_records, failed_records, valid_records, cid, lid)
	if upload_status == "processing" {
		result, err = stmt.Exec(upload_status, cid, lid, client_file_name, failed_file_path, total_records, failed_records, valid_records)
	} else {
		result, err = stmt.Exec(upload_status, client_file_name, failed_file_path, total_records, failed_records, valid_records, cid, lid)
	}
	if err != nil {
		ErrorLog(RequestID, err.Error())
		return false
	}

	numRowsAffected, err := result.RowsAffected()
	if err != nil {
		ErrorLog(RequestID, fmt.Sprintf("Error checking affected rows:%s", err))
		return false
	}

	return numRowsAffected > 0
}
