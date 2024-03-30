package couponfetch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	Utils "coupon/couponFetchApi/util"
	util "coupon/util"
)

func CouponFetchredis(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close() // Closing the request body to prevent resource leaks

	var apireq util.APIRequest
	json.Unmarshal(reqBody, &apireq)
	fmt.Println(apireq)
	Utils.InitDB()

	var Couponstr util.APIResponse
	if apireq.Cid == 0 || apireq.Coupon_list_id == 0 || apireq.Quantity == 0 || apireq.Request_id == "<nil>" {
		Couponstr.ResponseCode = 500
		Couponstr.Message = "Enter valid input in clientid/listid/quantity/Requestid "
	} else {
		Couponstr = Utils.GetcouponsRedis(apireq)
	}

	json.NewEncoder(w).Encode(Couponstr)
}
