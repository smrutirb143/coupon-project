package main

import (
	util "coupon/threshold-coupon-fetch/util"
	"fmt"
	"os"
	"syscall"
)

const lockFile = "/var/run/threshold_cron.lock"

func main() {
	// Check if lock file exists
	_, err := os.Stat(lockFile)
	if err == nil {
		fmt.Println("Process is already running. Exiting...")
		return
	}

	// Create lock file
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Error creating lock file:", err)
		return
	}
	defer file.Close()

	// Acquire an exclusive lock on the lock file
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		fmt.Println("Error acquiring lock:", err)
		os.Remove(lockFile)
		return
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	// Start the process
	fmt.Println("Starting the process...")

	util.Logger.WriteLog("INFO", "The threshold run started")
	res := util.GetThresholdDetails()
	if !res {
		os.Remove(lockFile)
		util.Logger.WriteLog("ERROR", "something unexpected occurred")
	}
	util.Logger.WriteLog("INFO", "The threshold run Ended")
	os.Remove(lockFile)
}
