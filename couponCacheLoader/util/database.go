package utils

import (
	util "coupon/util"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

	UUID = uuid.New().String()

	util.Logger.WriteLog("Info", fmt.Sprintf("%s:This is starting execution for coupon loader", UUID))

}

func getDBConnection() (*sql.DB, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	dbHost := util.Appconfig.RDS_HOSTNAME
	dbName := util.Appconfig.RDS_DBNAME
	dbUser := util.Appconfig.RDS_USERNAME
	dbPassword := util.Appconfig.RDS_PASSWORD
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPassword, dbHost, dbName)

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
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:MYSQL Connection successfully for loader ", UUID))
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
func getKeyLength(keyName string) (int, error) {
	conn := util.Redis_pool.Get()
	defer conn.Close()

	length, err := redis.Int(conn.Do("LLEN", keyName))
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s: The existing lenght of key %s is %d", UUID, keyName, length))
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured during getting length of %s", err.Error(), keyName))
		return 0, err
	}

	return length, nil
}

func SendRedis20Percent(request util.APIRequest) (bool, bool, error) {
	cid := request.Cid
	listID := request.Coupon_list_id
	count := 0
	var idArr []int
	CouponTable := getCouponStr(util.Appconfig.Table_substr, cid, listID)
	CouponRedisKey := getCouponStr(util.Appconfig.Coupon_key, cid, listID)
	existCount, err := getKeyLength(CouponRedisKey)
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s: table:%s,rediskey:%s,existcount:%d", UUID, CouponTable, CouponRedisKey, existCount))
	if err != nil {
		return false, false, fmt.Errorf("error getting key length for Redis key %s: %v", CouponRedisKey, err)
	}
	conn := util.Redis_pool.Get()
	defer conn.Close()
	var remaining int
	if conn == nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured during connecting to redis %s while fetching coupon", UUID, err))
		return false, false, fmt.Errorf("failed to obtain a new Redis connection")
	}
	remaining = request.Quantity
	if request.Call_form == 0 {
		if remaining == 0 || existCount >= (request.Quantity+1) {
			return true, true, nil
		}
		remaining = (request.Quantity + 1) - existCount
	}
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The redis length is less, hence getting remaining coupon %d ", UUID, remaining))
	if remaining > 0 {
		data, updateStatus, expiry := GetmysqlData(CouponTable, remaining, cid, listID)
		if !updateStatus || len(data) == 0 {
			if len(data) == 0 {
				util.Logger.WriteLog("INFO", fmt.Sprintf("%s:There are less coupon present than requested for client %d and listid %d ", UUID, cid, listID))
				return false, false, fmt.Errorf("There are less coupon present than requested")
			}
			return false, false, nil
		}
		var start int

		fmt.Println(data)
		datalen := len(data)
		iter := 0
		for id, couponStr := range data {
			iter++
			count++
			idArr = append(idArr, id)
			var value util.Mysqldata
			value.Code = couponStr
			value.Coupon_expiry = expiry
			value.Id = id
			redisData, _ := json.Marshal(value)
			redisString := string(redisData)
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The redis string is adding on redis pipeline %s", UUID, redisString))

			if err := conn.Send("RPUSH", CouponRedisKey, redisString); err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error adding SET command to transaction:%s", UUID, err))
				// Reset the pipeline
				if _, err := conn.Do("DISCARD"); err != nil {
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error discarding transaction:%s", UUID, err))
					continue
				}
			}

			if count == util.Appconfig.Batchsize || iter == datalen {
				if _, err := conn.Do("MULTI"); err != nil {
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:error starting transaction for client:%d is %s", UUID, cid, err))
					return false, false, fmt.Errorf("error starting transaction: %v", err)
				}
				start++
				util.Logger.WriteLog("INFO", fmt.Sprintf("%s:%d batch started for table %s", UUID, start, CouponTable))
				// Execute pipeline
				_, err := conn.Do("EXEC")
				if err != nil {
					conn.Do("DISCARD")
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error executing pipeline,redis push failed:%s", UUID, err))
					BatchUpdate(idArr, CouponTable, util.Appconfig.Used_status)
					// Rollback previous updates
					return false, false, nil
				}
				// Commit updates
				err = BatchUpdate(idArr, CouponTable, util.Appconfig.Used_status)
				if err != nil {
					util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error committing updates,hence making status as available:%s", UUID, err))
					return false, false, nil

				}
				idArr = []int{}
				count = 0
			}
		}
		return true, true, nil
	}
	return true, true, nil
}

func BatchUpdate(Coupon_id []int, Coupon_table string, status string) error {
	maxRetries := util.Appconfig.MAXRETRY

	query := fmt.Sprintf("UPDATE %s SET status = ? WHERE id IN (%s)",
		Coupon_table, strings.Trim(strings.Repeat("?,", len(Coupon_id)), ","))

	stmt, err := Mysqldb.Prepare(query)
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s: Error in preparing sql statement %s", UUID, err))
		return fmt.Errorf("error preparing SQL query: %v", err)
	}
	defer stmt.Close()

	for retries := 0; retries < maxRetries; retries++ {
		params := make([]interface{}, len(Coupon_id)+1)
		params[0] = status
		for i, id := range Coupon_id {
			params[i+1] = id
		}
		util.Logger.WriteLog("INFO", fmt.Sprintf("%s:The params are %s with status ", params...))
		_, err = stmt.Exec(params...)
		if err == nil {
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Update status to used write successful for %s ", UUID, Coupon_table))
			return nil // Successful execution
		}

		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error executing query (retry %d/%d): %v\n", UUID, retries+1, maxRetries, err))
		time.Sleep(time.Second) // Adding delay before retry
	}

	return fmt.Errorf("maximum retries exceeded")
}

func GetmysqlData(Coupon_table string, db_length int, cid int, listID int) (map[int]string, bool, string) {
	var Coupon_code string
	var expiry string
	id_map := make(map[int]string)
	Mysqldb, err := getDBConnection()
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:The error while getting mysql connection %s", UUID, err))
		return id_map, false, expiry
	}

	var retryCount int
	maxRetries := util.Appconfig.MAXRETRY
	for retryCount < maxRetries {
		query := fmt.Sprintf("SELECT coupon_code, id FROM %s WHERE status IN ('available') ORDER BY id ASC LIMIT %d", Coupon_table, db_length)
		query1 := fmt.Sprintf("SELECT expiry FROM %s WHERE Cid=%d AND listID=%d", util.Appconfig.Meta_data_table, cid, listID)

		err = Mysqldb.QueryRow(query1).Scan(&expiry)
		if err != nil {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error retrieving expiry information after maximum retries:%s", UUID, err))
			return id_map, false, expiry
		}

		results, err := Mysqldb.Query(query)
		if err != nil {
			retryCount++
			if retryCount == maxRetries {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error executing query after maximum retries:%s", UUID, err))
				return id_map, false, expiry
			}
			time.Sleep(time.Second)
			continue
		}

		for results.Next() {
			var id int
			err = results.Scan(&Coupon_code, &id)
			if err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s: Error occured while scanning result %s", UUID, err.Error()))
				continue
			}
			id_map[id] = Coupon_code

		}
		return id_map, true, expiry
	}

	return id_map, false, expiry
}

func updateStatusused(Coupon_table string, status string, id int) bool {
	maxRetries := util.Appconfig.MAXRETRY
	retries := 0

	for {
		query := fmt.Sprintf("UPDATE %s SET status='%s' WHERE id = %d", Coupon_table, status, id)
		fmt.Println("Updating status ", status, "for id", id)

		result, err := Mysqldb.Exec(query)
		if err != nil {
			fmt.Println("Error executing update query:", err)
			if retries < maxRetries {
				fmt.Printf("Retrying update (attempt %d of %d)\n", retries+1, maxRetries)
				retries++
				time.Sleep(1 * time.Second)
				continue
			} else {
				fmt.Println("Maximum retry attempts reached.")
				return false
			}
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Println("Error getting rows affected:", err)
			return false
		}

		fmt.Printf("Update successful. Rows affected: %d\n", rowsAffected)
		return true
	}
}

func UpdateUsedCount(used, cid, listid int) bool {
	maxRetries := util.Appconfig.MAXRETRY
	retries := 0

	for {
		query := fmt.Sprintf("update coupon_store_meta set available_coupon=(available_coupon-%d) where cid=%d and listid=%d order by id desc limit 1", used, cid, listid)
		fmt.Println("Updating used count ", used)

		result, err := Mysqldb.Exec(query)
		if err != nil {
			fmt.Println("Error executing update query:", err)
			if retries < maxRetries {
				fmt.Printf("Retrying update (attempt %d of %d)\n", retries+1, maxRetries)
				retries++
				time.Sleep(1 * time.Second)
				continue
			} else {
				fmt.Println("Maximum retry attempts reached.")
				return false
			}
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Println("Error getting rows affected:", err)
			return false
		}

		fmt.Printf("Update successful. Rows affected: %d\n", rowsAffected)
		return true
	}

}
