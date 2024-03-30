package config

import "time"

const (
	Log_file       = "coupon_management.log"
	Direct         = false
	MAXRETRY       = 3
	Table_substr   = "coupon_store_"
	Valid_file_row = 2
	Primary_status = "available"
	Redis_host     = "redis-ingress-stg1.netcorein.com:6379"
	Coupon_key     = "COUPON_"
	Threshold      = 5 // threshold=(keeping 5 for 20% coupon to be pushed to redis, if 50%, then the constant value to be 2)
	RedisTimeout   = 5 * time.Second
	Batchsize      = 200
	BadChar        = `[\t\r\x1b\x00\x03\x08]`
	Used_status    = "used"
	Cached_status  = "cacheed"
)
