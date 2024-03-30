package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	Utils "couponManagement/utils"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	start := time.Now()
	lc, _ := lambdacontext.FromContext(ctx)
	RequestID := lc.AwsRequestID
	body := fmt.Sprint(request.Body)
	var insertedCount int

	Utils.InfoLog(RequestID, fmt.Sprint(body))
	// Extracting parameters from the HTTP request

	// Print the value of the environment variable

	var data map[string]interface{}
	err := json.Unmarshal([]byte(body), &data)
	if err != nil {
		Utils.ErrorLog(RequestID, fmt.Sprint(err))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprintf("error in json parsing"),
		}, nil
	}
	fileName := fmt.Sprint(data["couponFile"])
	listid := fmt.Sprint(data["list_id"])
	clientid := fmt.Sprint(data["client_id"])
	Type := fmt.Sprint(data["type"])
	tag := fmt.Sprint(data["type"])
	fallbackCode := fmt.Sprint(data["fallbackCode"])
	expiry := fmt.Sprint(data["expiry"])

	if listid == "<nil>" || clientid == "<nil>" || Type == "<nil>" || expiry == "<nil>" || fileName == "<nil>" {
		Utils.ErrorLog(RequestID, fmt.Sprint("The request does not contail listid or clientid or Type  or expiry", body))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint("The request does not contain listid or clientid or Type  or expiry or fileName "),
		}, nil
	}

	parsedTime, err := time.Parse("060102150405", expiry)
	if err != nil {
		Utils.ErrorLog(RequestID, fmt.Sprint("The expiry time should be YYMMDDHHMMSS", body))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint(" The expiry time should be YYMMDDHHMMSS "),
		}, nil
	}

	expired_time := parsedTime.Format("2006-01-02 15:04:05")
	table_name := Utils.Get_tableName_from_file(RequestID, listid, clientid)

	fileContent, err := Utils.GetFileFromS3(RequestID, fileName, true)
	if fileContent == nil || err != nil {
		Utils.ErrorLog(RequestID, fmt.Sprint("Some issue with readfile from s3", err))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint(" Some issue with readfile from s3, Kindly contact Netcore Support team "),
		}, nil
	}
	log := Utils.Client_upload_logs("processing", clientid, listid, fileName, "", 0, 0, 0, RequestID)
	if !log {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint("something went wrong while updating upload logs stage1"),
		}, nil

	}
	Content := Utils.RemoveBadCharacters(fmt.Sprintf(string(fileContent)))

	lines := strings.Split(Content, "\n")
	field := strings.Split(lines[0], ",")
	if len(field) > 1 || len(lines) < 2 {
		log = Utils.Client_upload_logs("failed", clientid, listid, fileName, "", 0, 0, 0, RequestID)
		Utils.ErrorLog(RequestID, fmt.Sprint("no record found in file or multiple columns detected"))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint(RequestID, " no record found in file or multiple columns detected"),
		}, nil

	}
	Utils.InfoLog(RequestID, fmt.Sprintf("The number of lines %d", len(lines)))
	var table_flag bool
	if len(lines) > 2 {
		table_flag = Utils.Create_table(RequestID, table_name)
		if !table_flag {
			log = Utils.Client_upload_logs("failed", clientid, listid, fileName, "", 0, 0, 0, RequestID)
			Utils.ErrorLog(RequestID, fmt.Sprint("issue in creating table, please contact support team"))
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       fmt.Sprint(RequestID, " issue in creating table, please contact support team "),
			}, nil
		}
		insertedCount = Utils.Bulkinsert(RequestID, lines, Type, tag, fallbackCode, expired_time, table_name)
		if insertedCount == 0 {
			Utils.ErrorLog(RequestID, fmt.Sprint("Zero Records inserted, need to check files or some issue occured or no new record found "))
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       fmt.Sprint(RequestID, " Zero Records inserted, need to check files or some issue occured or no new record found "),
			}, nil
		}
		Utils.InfoLog(RequestID, fmt.Sprintf("inserted count %d", insertedCount))
		log := Utils.Client_upload_logs("done", clientid, listid, fileName, "", len(lines), len(lines)-insertedCount, insertedCount, RequestID)
		if !log {
			Utils.ErrorLog(RequestID, fmt.Sprint("something went wrong while updating upload logs during done status"))
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusBadRequest,
				Body:       fmt.Sprint(RequestID, " something went wrong while updating upload logs"),
			}, nil

		}

	}

	update_status := Utils.Updatemetainfo(clientid, listid, insertedCount, Type, expired_time, fileName, fallbackCode, RequestID)
	if !update_status {
		Utils.ErrorLog(RequestID, fmt.Sprint("file uploaded, but logs did not inserted during insert into table meta"))
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprint(RequestID, " file uploaded, but logs did not inserted "),
		}, nil

	}
	duration := time.Since(start)
	Utils.InfoLog(RequestID, fmt.Sprintf("main execution time:%s", duration))

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       fmt.Sprintf(RequestID, "Successfully uploaded file"),
	}, nil

}

func main() {
	lambda.Start(handler)
}
