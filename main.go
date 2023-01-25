package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Basic Syslog POC
func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "514"
	}

	bufferLimit := 3
	if os.Getenv("SHUFFLE_LOG_BUFFER") != "" {
		val, err := strconv.Atoi(os.Getenv("SHUFFLE_LOG_BUFFER"))
		if err != nil {
			bufferLimit = val
		}
	}

	if os.Getenv("SHUFFLE_WEBHOOK") == "" {
		log.Fatal("SHUFFLE_WEBHOOK environment not set. Nowhere to send logs")
		os.Exit(1)
	}

	if !strings.HasPrefix(os.Getenv("SHUFFLE_WEBHOOK"), "http") {
		log.Fatal("SHUFFLE_WEBHOOK environment not set to a valid URL")
		os.Exit(1)
	}

	log.Printf("Listening on UDP port %s with %d logs to buffer before forwarding. ", port, bufferLimit)
	if err := serve(context.Background(), fmt.Sprintf("0.0.0.0:%s", port), bufferLimit); err != nil {
		log.Printf("[ERROR] Serve error: %v", err)
		os.Exit(1)
	}
}

type AccessLog struct {
	Host   string `json:"host"`
	IP     string `json:"ip"`
	Method string `json:"method"`

	Time   string `json:"time"`
	RawLog string `json:"raw_log"`
	UUID   string `json:"uuid"`
}

func forwardAndClear(accessLogRequest []AccessLog) error {
	accessLogRequest = []AccessLog{}

	// Marshal logs
	accessLogRequestBytes, err := json.Marshal(accessLogRequest)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal access-log request with '%v'", err)
		return err
	}

	// Forward to shuffle as HTTP request
	// Define client
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	url := os.Getenv("SHUFFLE_WEBHOOK")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(accessLogRequestBytes))
	if err != nil {
		log.Printf("[ERROR] Failed to create HTTP request with '%v'", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Failed to send HTTP request with '%v'", err)
		return err
	}

	if resp.StatusCode != 200 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		newStr := buf.String()
		log.Printf("[ERROR] Failed to send HTTP request. Status code: %d. Body: %#v", resp.StatusCode, newStr)

		// Read response body
		// Print body

		return errors.New("Failed to send HTTP request")
	}

	log.Printf("Forwarded %d logs", len(accessLogRequest))

	return nil
}

func handleAccessLog(accessLogs []AccessLog, buffer []byte) ([]AccessLog, error) {

	fixedBytes := []byte{}
	for _, char := range buffer {
		if char == 0 {
			continue
		}

		if char == 10 && len(fixedBytes) > 0 {
			accessLogs = append(accessLogs, AccessLog{
				Time:   time.Now().Format(time.RFC3339),
				RawLog: string(fixedBytes),
				UUID:   uuid.New().String(),
			})

			fixedBytes = []byte{}
			continue
		}

		//log.Println(char)
		fixedBytes = append(fixedBytes, char)
	}

	if len(fixedBytes) > 1 {
		accessLogs = append(accessLogs, AccessLog{
			Time:   time.Now().Format(time.RFC3339),
			RawLog: string(fixedBytes),
			UUID:   uuid.New().String(),
		})
	}

	return accessLogs, nil
}

func serve(ctx context.Context, address string, bufferLimit int) error {
	pc, err := net.ListenPacket("udp", address)
	if err != nil {
		log.Errorf("failed to UDP listen on '%s' with '%v'", address, err)
		return err
	}
	defer func() {
		if err := pc.Close(); err != nil {
			log.Errorf("failed to close packet connection with '%v'", err)
		}
	}()

	errChan := make(chan error, 1)
	// maxBufferSize specifies the size of the buffers that
	// are used to temporarily hold data from the UDP packets
	// that we receive.
	accessLogs := []AccessLog{}
	go func() {
		for {
			buffer := make([]byte, 2048)
			_, _, err := pc.ReadFrom(buffer)
			if err != nil {
				errChan <- err
				return
			}

			accessLogs, err = handleAccessLog(accessLogs, buffer)
			if err != nil {
				errChan <- err
				return
			}

			log.Printf("Log amounts: %d", len(accessLogs))
			if len(accessLogs) > bufferLimit {
				err = forwardAndClear(accessLogs)
				if err != nil {
					log.Printf("Forwarding error: %v", err)
					continue
				} else {
					accessLogs = []AccessLog{}
				}
			}
		}
	}()

	var ret error
	select {
	case <-ctx.Done():
		ret = ctx.Err()
		log.Infof("Cancelled with '%v'", err)
	case ret = <-errChan:
	}

	return ret
}
