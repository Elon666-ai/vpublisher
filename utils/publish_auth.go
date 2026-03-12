package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ResolvePublishURL fetches auth credentials from authApiAddr (POST)
// and replaces <user>/<password> placeholders in publishURL.
// If authApiAddr is empty, publishURL is returned as-is.
func ResolvePublishURL(authApiAddr, appSecret, publishURL string) (string, error) {
	authApiAddr = strings.TrimSpace(authApiAddr)
	appSecret = strings.TrimSpace(appSecret)
	if authApiAddr == "" {
		if appSecret == "" {
			return publishURL, nil
		}
	}

	app, stream, err := parseAppAndStreamFromPublishURL(publishURL)
	if err != nil {
		return "", err
	}
	// fmt.Printf("streamPath=%s/%s\n", app, stream)

	var user, password string
	if appSecret != "" {
		txTime := strconv.FormatInt(time.Now().Unix()+24*3600, 16)
		user = "u" + txTime
		password = GenTxSecret(appSecret, app+"/"+stream, txTime)
	} else {
		user, password, err = fetchPublishCredential(authApiAddr, app, stream)
		if err != nil {
			fmt.Println(err)
			return "", err
		}
	}
	if user == "" || password == "" {
		return "", fmt.Errorf("empty user/password")
	}

	resolved := strings.ReplaceAll(publishURL, "<user>", url.QueryEscape(user))
	resolved = strings.ReplaceAll(resolved, "<password>", url.QueryEscape(password))
	return resolved, nil
}

func fetchPublishCredential(authApiAddr, app, stream string) (string, string, error) {
	reqBody := authGenReq{
		AppId:  app,
		Stream: stream,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest(http.MethodPost, authApiAddr, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", "", fmt.Errorf("build auth request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("call auth api failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", "", fmt.Errorf("read auth response failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("auth api status=%d body=%s", resp.StatusCode, string(body))
	}
	// fmt.Println(string(body))

	var rsp apiResp
	if err := json.Unmarshal(body, &rsp); err != nil {
		return "", "", fmt.Errorf("parse auth response failed: %w", err)
	}
	// fmt.Println(rsp)

	return strings.TrimSpace(rsp.Data.User), strings.TrimSpace(rsp.Data.Password), nil
}

func parseAppAndStreamFromPublishURL(publishURL string) (string, string, error) {
	u, err := url.Parse(strings.TrimSpace(publishURL))
	if err != nil {
		return "", "", fmt.Errorf("parse publishUrl failed: %w", err)
	}

	streamID := strings.TrimSpace(u.Query().Get("streamid"))
	if streamID == "" {
		return "", "", fmt.Errorf("publishUrl missing streamid")
	}

	// expected pattern: publish:app/stream[:user[:password]]
	if idx := strings.Index(streamID, ":"); idx >= 0 {
		streamID = streamID[idx+1:]
	}
	parts := strings.SplitN(streamID, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid streamid format: %s", streamID)
	}
	app := strings.TrimSpace(parts[0])
	streamPart := strings.TrimSpace(parts[1])
	if app == "" || streamPart == "" {
		return "", "", fmt.Errorf("invalid streamid app/stream: %s", streamID)
	}

	stream := streamPart
	if idx := strings.Index(stream, ":"); idx >= 0 {
		stream = strings.TrimSpace(stream[:idx])
	}
	if stream == "" {
		return "", "", fmt.Errorf("invalid stream value in streamid: %s", streamID)
	}

	return app, stream, nil
}
