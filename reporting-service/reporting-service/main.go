package reporting

import (
	utils "coupon/reporting-service/util"
	util "coupon/util"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

func ReportApi(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	var request util.Request
	var response util.Response_arr
	json.Unmarshal(reqBody, &request)
	defer util.Logger.Close()
	utils.InitDB()

	if request.Cid == 0 || request.Coupon_list_ids == "" || request.Coupon_list_ids == "<nil>" {
		response.Response_code = 500
		response.Message = "Requested operation failed due to Cid is empty or coupon_list id is empty "

	}
	var reqResponse util.Response_arr
	reqResponse = utils.GetReportStats(request)
	json.NewEncoder(w).Encode(reqResponse)

}

func getUploadStatus(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	var request util.Uploadreq
	var resp util.UploadResponse
	json.Unmarshal(reqBody, &request)
	resp = utils.GetallUploadStatus(request)
	json.NewEncoder(w).Encode(resp)

}
