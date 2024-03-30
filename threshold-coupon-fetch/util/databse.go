package util

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	login "coupon/logconfig"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

var Logger *login.Logger

var (
	Mysqldb     *sql.DB
	UUID        string
	dbMutex     sync.Mutex
	dbInitiated bool
)

func init() {
	//initFluent()
	var err error
	InitConfig()
	Logger, err = login.OpenLog(Appconfig.Logfile)
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
	}
	UUID = uuid.New().String()

	Logger.WriteLog("Info", fmt.Sprintf("%s:This is starting execution", UUID))

}

func getDBConnection() (*sql.DB, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	dbHost := Appconfig.RDS_HOSTNAME
	dbName := Appconfig.RDS_DBNAME
	dbUser := Appconfig.RDS_USERNAME
	dbPassword := Appconfig.RDS_PASSWORD
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPassword, dbHost, dbName)

	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if !dbInitiated {
			Mysqldb, err = sql.Open("mysql", connectionString)
			if err != nil {
				Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured during connection %s", UUID, err))
				time.Sleep(1 * time.Second)
				continue
			}
			dbInitiated = true
		}

		err = Mysqldb.Ping()
		if err == nil {
			Logger.WriteLog("INFO", fmt.Sprintf("%s:Connection successfully ", UUID))
			return Mysqldb, nil
		}

		Mysqldb.Close()
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to establish database connection after 3 attempts")
}

func GetThresholdDetails() bool {
	threshold := Appconfig.ThresholdValue
	defer Mysqldb.Close()
	query := fmt.Sprintf("select cid,listID,round(available_coupon/%d) as threshold from coupon_store_meta where available_coupon>0", threshold)
	Conn, err := getDBConnection()
	if err != nil {
		Logger.WriteLog("ERROR", fmt.Sprintf("%s: Issue with MySQL connection: %s", UUID, err.Error()))
		return false
	}
	results, err := Conn.Query(query)
	if err != nil {
		Logger.WriteLog("ERROR", fmt.Sprintf("%s: The error occured while getting the o/p of query %s", UUID, query))
		return false
	}
	for results.Next() {
		var Oneclient Client_threshold
		err := results.Scan(&Oneclient.Cid, &Oneclient.Listid, &Oneclient.Threshold)
		Logger.WriteLog("INFO", fmt.Sprintf("%s: The details are  cid listid threshold %d,%d,%d", UUID, Oneclient.Cid, Oneclient.Listid, Oneclient.Threshold))
		if Oneclient.Cid == 0 || Oneclient.Listid == 0 || Oneclient.Threshold == 0 {
			continue
		}
		if err != nil {

			Logger.WriteLog("ERROR", fmt.Sprintf(" %s:Error while picking threshold %s for client %d hence continueing", UUID, err.Error(), Oneclient.Cid))
			continue
		}
		err = hitPostRequest(Oneclient)
		if err != nil {
			Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error occured while loading cache %s fro client %d, hence continueing", UUID, err, Oneclient.Cid))
			continue
		}

	}
	return true
}

func hitPostRequest(Oneclient Client_threshold) error {
	requestBody := map[string]int{
		"Cid":            Oneclient.Cid,
		"Coupon_list_id": Oneclient.Listid,
		"Quantity":       Oneclient.Threshold,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %v", err)
	}

	url := Appconfig.LoadUrl

	for retries := 3; retries > 0; retries-- {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			Logger.WriteLog("ERROR", fmt.Sprintf("%s:Error executing request (attempt %d/%d): %v\n", UUID, 4-retries, 3, err))
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			Logger.WriteLog("INFO", fmt.Sprintf("%s:Request successful for cid %d", UUID, Oneclient.Cid))
			return nil
		} else {
			Logger.WriteLog("ERROR", fmt.Sprintf("%s:Request failed with status code %d (attempt %d/%d)\n", UUID, resp.StatusCode, 4-retries, 3))
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("failed to get 200 response code after 3 attempts")
}
