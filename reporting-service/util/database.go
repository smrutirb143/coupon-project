package util

import (
	util "coupon/util"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

	util.Logger.WriteLog("Info", fmt.Sprintf("%s:This is starting execution", UUID))

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
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:MYSQL Connection successfully for report ", UUID))
			return Mysqldb, nil
		}

		Mysqldb.Close()
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to establish database connection after 3 attempts")
}

func GetReportStats(request util.Request) util.Response_arr {
	var res util.Response_arr
	res.Response_code = 400

	Conn, err := getDBConnection()
	if err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s: Issue with MySQL connection:%s", UUID, err.Error()))
		res.Message = "db connection failed"
		res.Response_code = 500
		return res
	}

	var listids []int
	if err := json.Unmarshal([]byte(request.Coupon_list_ids), &listids); err != nil {
		util.Logger.WriteLog("ERROR", fmt.Sprintf("%s: Issue with json parsing:%s", UUID, err.Error()))
		res.Message = "Issue with json parsing:"
		res.Response_code = 500
		return res
	}
	commaSeparatedString := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(listids)), ","), "[]")
	util.Logger.WriteLog("INFO", fmt.Sprintf("%s: The cid listid and requestid are %d,%s,%s", UUID, request.Cid, request.Coupon_list_ids, request.Request_id))

	query := fmt.Sprintf("SELECT listID, (total_coupon-available_coupon) AS sent, available_coupon AS available, type AS coupon_type FROM coupon_store_meta WHERE listID IN (%s)", commaSeparatedString)

	// Retry logic
	retries := util.Appconfig.MAXRETRY
	for retries > 0 {
		result, err := Conn.Query(query)
		if err != nil {
			util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error executing query:%s", UUID, err.Error()))
			retries--
			util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Retrying... Attempts left: %d\n", UUID, retries))
			continue
		}

		var list []util.FileMeta
		for result.Next() {
			var response util.FileMeta
			err := result.Scan(&response.Coupon_list_id, &response.Sent, &response.Available, &response.Coupon_type)
			util.Logger.WriteLog("INFO", fmt.Sprintf("The file mera are %d,%d,%d,%s", response.Coupon_list_id, response.Sent, response.Available, response.Coupon_type))
			if err != nil {
				util.Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error scanning row:%s", UUID, err))
				continue
			}
			list = append(list, response)
		}

		if len(list) > 0 {
			res.List = list
			res.Response_code = 200
			res.Message = "Operation successful"

			return res
		}

		retries--
		util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Retrying... Attempts left: %d\n", UUID, retries))
	}

	util.Logger.WriteLog("INFO", fmt.Sprintf("%s:Failed to execute query after %d attempts", UUID, util.Appconfig.MAXRETRY))
	res.Message = "Failed to execute query after 3 attempts"
	return res
}

func GetallUploadStatus(request util.Uploadreq) util.UploadResponse {
	var list util.UploadResponse
	Conn, err := getDBConnection()
	if err != nil {
		fmt.Println("Issue with MySQL connection:", err.Error())
		list.Message = "Error is connection"
		list.Response_code = 500
		return list
	}
	query := fmt.Sprintf("select client_file_name,total_records,failed_records,valid_records,failed_file_path from coupon_upload_logs where clientid=%d;", request.Cid)
	retries := util.Appconfig.MAXRETRY
	for retries > 0 {
		result, err := Conn.Query(query)
		if err != nil {
			fmt.Println("Error executing query:", err.Error())
			retries--
			fmt.Printf("Retrying... Attempts left: %d\n", retries)
			continue
		}

		for result.Next() {
			var resp util.UploadRes
			err := result.Scan(&resp.FileName, &resp.TotalRecords, &resp.FailedRecords, &resp.ValidRecords, &resp.FailedFilePath)
			if err != nil {
				fmt.Println("Error scanning row:", err.Error())
				continue
			}
			list.List = append(list.List, resp)
		}

		if len(list.List) > 0 {
			list.Response_code = 200
			list.Message = "Operation successful"
			return list
		}

		retries--
		fmt.Printf("Retrying... Attempts left: %d\n", retries)
	}

	fmt.Println("Failed to execute query after", util.Appconfig.MAXRETRY, "attempts")
	list.Message = "Failed to execute query after 3 attempts"
	return list

}
