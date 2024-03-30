package couponCacheloader

import (
	Utils "coupon/couponCacheLoader/util"
	util "coupon/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func CouponCacheLoad(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body)
	var apireq util.APIRequest

	json.Unmarshal(reqBody, &apireq)
	fmt.Println(apireq)
	var SuccessCode bool
	var coupon_exist bool
	var error error
	Utils.InitDB()

	var response util.BulkApiResponse
	if apireq.Cid == 0 || apireq.Coupon_list_id == 0 {
		response.ResponseCode = 400
		response.Message = "Clientid and listid mandatory or log file not created "
		json.NewEncoder(w).Encode(response)
	}
	SuccessCode, coupon_exist, error = Utils.SendRedis20Percent(apireq)
	if error != nil {
		response.Message = error.Error()
		response.ResponseCode = 400

	}

	if response.Message == "" {
		if SuccessCode {
			response.ResponseCode = 200
			response.Message = "record insrted for list"
		} else {
			response.ResponseCode = 500
			response.Message = "Threshold coupon pushed to redis failed"
		}

		if !coupon_exist && response.Message == "" {
			response.Message = "Less coupon are there in cache//db than the batchlimit"
		}
	}
	json.NewEncoder(w).Encode(response)

}
