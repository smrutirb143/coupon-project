package utils

import (
	"bytes"
	util "coupon/util"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

var (
	Mysqldb     *sql.DB
	dbMutex     sync.Mutex
	dbInitiated bool
	UUID        string
)

func InitDB() {
	//initFluent()

	UUID = uuid.New().String()

	util.Logger.WriteLog("Info", fmt.Sprintf("%s:This is starting execution for fetcher", UUID))

}

func getDBConnection() (*sql.DB, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	dbHost := util.Appconfig.RDS_HOSTNAME
	dbName := util.Appconfig.RDS_DBNAME
	dbUser := util.Appconfig.RDS_USERNAME
	dbPassword := util.Appconfig.RDS_PASSWORD
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPassword, dbHost, dbName)
	fmt.Println("The uuid is ", UUID)

	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if !dbInitiated {
			Mysqldb, err = sql.Open("mysql", connectionString)
			if err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured during connection %s", UUID, err))
				time.Sleep(1 * time.Second)
				continue
			}
			dbInitiated = true
		}

		err = Mysqldb.Ping()
		if err == nil {
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:MYSQL Connection successfully for fetch ", UUID))
			return Mysqldb, nil
		}

		Mysqldb.Close()
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to establish database connection after 3 attempts")
}

func getCouponStr(key string, cid int, listid int) string {

	tablename := fmt.Sprintf("%s%s_%s", key, strconv.Itoa(cid), strconv.Itoa(listid))
	return tablename
}

func GetcouponsRedis(request util.APIRequest) util.APIResponse {
	BatchCount := request.Quantity
	cid := request.Cid
	listID := request.Coupon_list_id
	requestid := request.Request_id
	var Couponstr []util.Mysqldata
	var final_data util.APIResponse

	keyName := getCouponStr(util.Appconfig.Coupon_key, cid, listID)
	length, _ := getKeyLength(keyName)
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The fetch from redis is going to start for %s for length %d", UUID, keyName, length))

	if length < BatchCount {

		//currently if redis_length>quantity, the get redis coupon else get coupon loader with remaining count
		//in future this will remain same but the loader will also occure via cache loader scheduler to load the threshold value
		err := hitPostRequest(cid, listID, (BatchCount - length))
		len, _ := getKeyLength(keyName)
		fmt.Println("The redis length is ", len, "   ", BatchCount)
		if len < BatchCount {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:The requested coupon is grater than the coupon present in redis %s", UUID, keyName))
			final_data.Message = ":The requested coupon is grater than the coupon present in redis "
			final_data.ResponseCode = 400
			final_data.Status = "failed"
			return final_data
		}
		if err != nil {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:The error occured while fetching for %s", UUID, keyName))
			final_data.Message = "Could not fetch data, please contact support team"
			final_data.ResponseCode = 400
			final_data.Status = "failed"
			return final_data
		}
	}
	Couponstr = fetchcouponString(keyName, BatchCount)
	if len(Couponstr) == 0 {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Could not fetch data, please contact support team %s", UUID, keyName))
		final_data.Message = "Could not fetch data, please contact support team"
		final_data.ResponseCode = 400
		final_data.Status = "failed"
		return final_data
	}
	final_data = formatStruct(Couponstr)
	final_data.Request_id = requestid
	final_data.Coupon_list_id = listID
	final_data.Message = "Operation successful"
	final_data.Status = "Success"
	final_data.ResponseCode = 200
	used := len(final_data.Coupons)
	UpdateUsedCount(used, cid, listID)

	return final_data
}

func hitPostRequest(cid, couponListID, quantity int) error {
	requestBody := map[string]int{
		"Cid":            cid,
		"Coupon_list_id": couponListID,
		"Quantity":       quantity,
		"Call_form":      1,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %v", err)
	}

	url := util.Appconfig.LoadUrl

	for retries := 3; retries > 0; retries-- {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error executing request (attempt %d/%d): %v\n", 4-retries, 3, err)
			time.Sleep(1 * time.Second)
			continue
		}

		defer resp.Body.Close()

		// Print response body
		var responseBody bytes.Buffer
		_, err = responseBody.ReadFrom(resp.Body)
		if err != nil {
			fmt.Printf("Error reading response body: %v\n", err)
		}
		fmt.Println("Response Body:", responseBody.String())

		if resp.StatusCode == http.StatusOK {
			fmt.Println("Request successful")
			return nil
		} else {
			fmt.Printf("Request failed with status code %d (attempt %d/%d)\n", resp.StatusCode, 4-retries, 3)
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("failed to get 200 response code after 3 attempts")
}

/*
func retryFailedUpdates(ids []int, Expiry string, coupons []coupon_string, keyName string) {
	conn := Redis_pool.Get()
	defer conn.Close()
	var retry_data []Mysqldata
	var data Mysqldata
	for i := 0; i < len(ids); i++ {
		data.Coupon_code = coupons[i].Code
		data.Coupon_expiry = Expiry
		data.Id = ids[i]
		retry_data = append(retry_data, data)
	}
	for _, singledata := range retry_data {
		jsonBytes, _ := json.Marshal(singledata)
		jsonString := string(jsonBytes)

		fmt.Println("The single redis string is", jsonString)
		_, err := conn.Do("RPUSH", keyName, jsonString)
		if err != nil {
			fmt.Println("Error pushing back to Redis:", err)

		}

	}
}
*/

func formatStruct(Couponstr []util.Mysqldata) util.APIResponse {
	var couponCodes []util.Coupon_string
	var single_coupon util.Coupon_string
	var ids []int
	var final_str util.APIResponse
	for _, data := range Couponstr {
		single_coupon.Code = data.Code
		couponCodes = append(couponCodes, single_coupon)
		ids = append(ids, data.Id)
	}
	final_str.Coupons = couponCodes
	final_str.Expiry = Couponstr[0].Coupon_expiry
	final_str.Id = ids

	return final_str
}

func getKeyLength(keyName string) (int, error) {
	conn := util.Redis_pool.Get()
	defer conn.Close()

	length, err := redis.Int(conn.Do("LLEN", keyName))
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:The error occured during getting length %s", UUID, err))
		return 0, err
	}

	return length, nil
}

func fetchcouponString(key string, BatchCount int) []util.Mysqldata {
	var redisString []util.Mysqldata
	MaxRetries := util.Appconfig.MAXRETRY

	for retry := 0; retry < MaxRetries; retry++ {
		conn := util.Redis_pool.Get()
		defer conn.Close()

		var redisStr string
		for i := 1; i <= BatchCount; i++ {
			reply, err := redis.Values(conn.Do("BLPOP", key, int(util.Appconfig.RedisTimeout/time.Second)))
			if err != nil {
				if err == redis.ErrNil {
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:List is empty %s", UUID, err))
					fmt.Println("List is empty")
					break
				} else {
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured during blpop, with retry %d for key %s with error %s", UUID, retry, key, err))
					fmt.Println("Error:", err)
					continue
				}
			}

			if _, err := redis.Scan(reply, &key, &redisStr); err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error scanning reply, with retry %d for key %s with error %s", UUID, retry, key, err))
				fmt.Println("Error scanning reply:", err)
				continue
			}

			redisdata, err := unmarshalRedisJSON(redisStr)
			if err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error ocured during unmarshal json  with retry %d for key %s with error %s", UUID, retry, key, err))
				fmt.Println("Error unmarshalling JSON:", err)
				continue
			}
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The redis string blpop occured %+v", UUID, redisdata))

			redisString = append(redisString, redisdata)
		}

		if len(redisString) > 0 {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	return redisString
}

func unmarshalRedisJSON(redisString string) (util.Mysqldata, error) {
	// Unmarshal the JSON string into the struct

	var mysqlData util.Mysqldata
	err := json.Unmarshal([]byte(redisString), &mysqlData)
	if err != nil {

		return util.Mysqldata{}, err
	}
	return mysqlData, nil
}

/*
	func updateStatusused(ids []int, tablename string, status string, batchSize int) (bool, int) {
		var end int
		MaxRetries := util.Appconfig.MAXRETRY
		var start int
		Mysqldb, err := getDBConnection()
		if err != nil {
			fmt.Println(err)
			return false, end
		}

		for retry := 0; retry < MaxRetries; retry++ {
			for i := 0; i < len(ids); i += batchSize {
				end = i + batchSize
				if end > len(ids) {
					end = len(ids)
				}

				batch := ids[i:end]
				start = i

				query := fmt.Sprintf("UPDATE %s SET status = '%s' WHERE id IN (%s)", tablename, status, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(batch)), ","), "[]"))

				_, err := Mysqldb.Exec(query)
				if err != nil {
					fmt.Printf("Error executing update query: %s. Retrying...\n", err)
					break
				}
			}

			if end >= len(ids) {
				return true, end
			}

			time.Sleep(200 * time.Microsecond)
		}

		return false, start + 1 //failed start position
	}
*/
func UpdateUsedCount(used, cid, listid int) bool {
	maxRetries := util.Appconfig.MAXRETRY
	retries := 0
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The available  count is going to update updated with %d for client %d and for listid %d", UUID, used, cid, listid))
	Mysqldb, err := getDBConnection()
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:error occured during getting mysql connection %s", UUID, err))
		return false
	}

	for {
		query := fmt.Sprintf("update coupon_store_meta set available_coupon=(available_coupon-%d) where cid=%d and listid=%d order by id desc limit 1", used, cid, listid)
		util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Updating used count %d ", UUID, used))

		result, err := Mysqldb.Exec(query)
		if err != nil {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:The error occured during executing updateing count %s", UUID, err))
			fmt.Println("Error executing update query:", err)
			if retries < maxRetries {
				fmt.Printf("Retrying update (attempt %d of %d)\n", retries+1, maxRetries)
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Retrying update on retry number %d", UUID, retries))
				retries++
				time.Sleep(1 * time.Second)
				continue
			} else {
				util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Maximum retry attempts reached", UUID))
				fmt.Println("Maximum retry attempts reached.")
				return false
			}
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error getting rows affected", UUID))
			fmt.Println("Error getting rows affected:", err)
			return false
		}

		fmt.Printf("Update successful. Rows affected: %d\n", rowsAffected)
		util.Logger.WriteLog("INFO", fmt.Sprintf("%s: Update successful in coupon_store_meta for cid %d and listid %d", UUID, cid, listid))
		return true
	}
}
