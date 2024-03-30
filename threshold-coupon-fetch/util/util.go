package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

type Client_threshold struct {
	Cid       int `json:"cid"`
	Listid    int `json:"listid"`
	Threshold int `json:"threshold"`
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
	ThresholdValue   int           `yaml:"ThresholdValue"`
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
