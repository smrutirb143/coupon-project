package util

import (
	login "coupon/logconfig"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/yaml.v2"
)

var Redis_pool *redis.Pool
var Logger *login.Logger

func init() {
	InitConfig()
	var err error
	Logger, err = login.OpenLog(Appconfig.Logfile)
	if err != nil {
		fmt.Printf("Failed to create logger: %v", err)
		os.Exit(0)
	}
	initRedisPool()

}

func initRedisPool() {
	var err error

	Redis_pool = NewPool(Appconfig.Redis_host)
	_, err = Redis_pool.Dial()
	if err != nil {
		Logger.WriteLog("ERROR", fmt.Sprintf("REDISFAIL: Could not connect to Redis_host redisserver  %v", err))
		fmt.Printf("\n Initiatate redis pool ERROR : %v", err)
	}
}

func NewPool(server string) *redis.Pool {

	var maxIdle int
	var maxActive int
	var err error

	if maxIdle, err = strconv.Atoi(Appconfig.REDIS_MAX_IDLE); err != nil {
		maxIdle = 5
	}

	if maxActive, err = strconv.Atoi(Appconfig.REDIS_MAX_ACTIVE); err != nil {
		maxActive = 8
	}

	Logger.WriteLog("INFO", fmt.Sprintf("RedisPool MaxIdeal:%d, MaxActive: %d", maxIdle, maxActive))
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 30 * time.Second,
		Wait:        true,
		MaxActive:   maxActive,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				Logger.WriteLog("ERROR", "Couldnot connect to redis Error : "+err.Error())
				return nil, err
			}
			return c, err
		},
	}
}

var fluentTag string
var Flogger *fluent.Fluent

type APIRequest struct {
	Cid            int    `json:"cid"`
	Coupon_list_id int    `json:"coupon_list_id"`
	Quantity       int    `json:"quantity"`
	Request_id     string `json:"request_id"`
	Call_form      int    `json:"Call_form"`
}
type Coupon_string struct {
	Code string `json:"code"`
}
type APIResponse struct {
	ResponseCode   int32           `json:"response_code"`
	Status         string          `json:"status"`
	Message        string          `json:"message"`
	Coupons        []Coupon_string `json:"coupons"`
	Expiry         string          `json:"coupon_expiry"`
	Id             []int           `json:"id"`
	Coupon_list_id int             `json:"coupon_list_id"`
	Request_id     string          `json:"request_id"`
}
type BulkApiResponse struct {
	ResponseCode int32  `json:"response_code"`
	Message      string `json:"message"`
	//,omitempty
}

type Mysqldata struct {
	Code          string `json:"code"`
	Coupon_expiry string `json:"coupon_expiry"`
	Id            int    `json:"id"`
}
type Request struct {
	Cid             int    `json:"cid"`
	Coupon_list_ids string `json:"coupon_list_ids"`
	Request_id      string `json:"request_id"`
}
type FileMeta struct {
	Coupon_list_id int    `json:"coupon_list_id"`
	Sent           int    `json:"sent"`
	Available      int    `json:"available"`
	Coupon_type    string `json:"coupon_type"`
}

type Response_arr struct {
	Status        string     `json:"status"`
	Response_code int        `json:"code"`
	Message       string     `json:"message"`
	List          []FileMeta `json:"list"`
	Request_id    string     `json:"request_id"`
}

type Uploadreq struct {
	Cid int `json:"cid"`
}

type UploadRes struct {
	FileName       string `json:"fileName"`
	TotalRecords   int    `json:"totalRecords"`
	FailedRecords  int    `json:"failedRecords"`
	ValidRecords   int    `json:"validRecords"`
	FailedFilePath string `json:"failedFilePath"`
}
type UploadResponse struct {
	List          []UploadRes `json:"Listfile"`
	Response_code int         `json:"code"`
	Message       string      `json:"message"`
}

type Client_config struct {
	MAXRETRY         int           `yaml:"MAXRETRY"`
	Table_substr     string        `yaml:"Table_substr"`
	Primary_status   string        `yaml:"Primary_status"`
	Redis_host       string        `yaml:"Redis_host"`
	Coupon_key       string        `yaml:"Coupon_key"`
	Threshold        int           `yaml:"Threshold"`
	RedisTimeout     time.Duration `yaml:"RedisTimeout"`
	Batchsize        int           `yaml:"Batchsize"`
	Used_status      string        `yaml:"Used_status"`
	RDS_HOSTNAME     string        `yaml:"RDS_HOSTNAME"`
	RDS_DBNAME       string        `yaml:"RDS_DBNAME"`
	RDS_USERNAME     string        `yaml:"RDS_USERNAME"`
	RDS_PASSWORD     string        `yaml:"RDS_PASSWORD"`
	REDIS_MAX_IDLE   string        `yaml:"REDIS_MAX_IDLE"`
	REDIS_MAX_ACTIVE string        `yaml:"REDIS_MAX_ACTIVE"`
	Meta_data_table  string        `yaml:"Meta_data_table"`
	LoadPort         string        `yaml:"LoadPort"`
	FetchPort        string        `yaml:"FetchPort"`
	ReportPort       string        `yaml:"ReportPort"`
	LoadUrl          string        `yaml:"LoadUrl"`
	Logfile          string        `yaml:"Logfile"`
}

var Appconfig Client_config

func InitConfig() {
	// Get the config file path from command-line arguments
	if len(os.Args) < 2 {
		fmt.Println("Config file path argument missing")
		os.Exit(0)
	}
	configFile := os.Args[1]

	// Load configuration from the YAML file
	LoadConfig(configFile)
}

func LoadConfig(configFile string) {
	// Read YAML file
	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println("Error reading YAML file: ", err.Error())
	}

	// Unmarshal YAML data into AppConfig struct
	if err := yaml.Unmarshal(yamlFile, &Appconfig); err != nil {
		fmt.Println("Error unmarshaling YAML data: ", err.Error())
	}
	fmt.Println(Appconfig)
}
