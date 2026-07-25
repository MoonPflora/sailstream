package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type injectRequest struct {
	Platform    string `json:"platform"`
	SubtypeID   string `json:"subtype_id,omitempty"`
	AccountID   string `json:"account_id,omitempty"`
	UserID      string `json:"user_id"`
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Text        string `json:"text"`
	ChatJID     string `json:"chat_jid,omitempty"`
	ImagePath   string `json:"image_path,omitempty"`
}

type step struct {
	Type        string                 `json:"Type"`
	Value       string                 `json:"Value"`
	Description string                 `json:"Description"`
	Options     map[string]interface{} `json:"Options"`
	DelayAfter  int                    `json:"DelayAfter"`
}

type sandboxRecord struct {
	Instruction struct {
		Platform       string                 `json:"Platform"`
		SubtypeID      string                 `json:"SubtypeID"`
		TicketID       string                 `json:"TicketID"`
		NotificationID string                 `json:"NotificationID"`
		Action         string                 `json:"Action"`
		Intent         string                 `json:"Intent"`
		OriginalText   string                 `json:"OriginalText"`
		Data           map[string]interface{} `json:"Data"`
		Steps          []step                 `json:"Steps"`
	} `json:"instruction"`
	RawResult map[string]interface{} `json:"raw_result,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9099", "sandbox HTTP address of the main service")
	platform := flag.String("platform", "whatsapp", "whatsapp or telegram")
	subtype := flag.String("subtype", "sandbox", "subtype_id to inject as")
	userID := flag.String("user", "sandbox-user-1", "fake platform user id")
	username := flag.String("username", "", "fake username")
	displayName := flag.String("display-name", "Test User", "fake display name")
	chatJID := flag.String("chat-jid", "", "override the WhatsApp chat JID")
	flag.Parse()

	base := "http://" + *addr
	fmt.Printf("sandbox-cli connected to %s as %s (%s/%s)\n", base, *userID, *platform, *subtype)
	if *subtype == "sandbox" {
		fmt.Println("lane: fully isolated — nothing here touches a real WhatsApp/Telegram session")
	} else {
		fmt.Println("lane: LIVE — routing through your real running listener's queue")
	}
	fmt.Println("commands: /image <path>  /platform whatsapp|telegram  /subtype <id>  /quit")
	fmt.Println()

	pendingImage := ""
	curPlatform := *platform
	curSubtype := *subtype

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("you> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch {
		case line == "/quit" || line == "/exit":
			return
		case strings.HasPrefix(line, "/image "):
			path := strings.TrimSpace(strings.TrimPrefix(line, "/image "))
			if _, err := os.Stat(path); err != nil {
				fmt.Printf("  ! can't read %s locally (%v)\n", path, err)
			}
			pendingImage = path
			fmt.Printf("  attached image for next message: %s\n", pendingImage)
			continue
		case strings.HasPrefix(line, "/platform "):
			p := strings.TrimSpace(strings.TrimPrefix(line, "/platform "))
			if p != "whatsapp" && p != "telegram" {
				fmt.Println("  ! platform must be whatsapp or telegram")
				continue
			}
			curPlatform = p
			fmt.Printf("  switched to %s\n", curPlatform)
			continue
		case strings.HasPrefix(line, "/subtype "):
			s := strings.TrimSpace(strings.TrimPrefix(line, "/subtype "))
			if s == "" {
				fmt.Println("  ! usage: /subtype <id>  (or \"sandbox\" for the isolated lane)")
				continue
			}
			curSubtype = s
			if curSubtype == "sandbox" {
				fmt.Println("  switched to sandbox — isolated lane")
			} else {
				fmt.Printf("  switched to subtype %q — LIVE lane\n", curSubtype)
			}
			continue
		}

		req := injectRequest{
			Platform:    curPlatform,
			SubtypeID:   curSubtype,
			UserID:      *userID,
			Username:    *username,
			DisplayName: *displayName,
			Text:        line,
			ChatJID:     *chatJID,
			ImagePath:   pendingImage,
		}
		pendingImage = ""

		if err := send(base, req); err != nil {
			fmt.Printf("  ! inject failed: %v\n", err)
			continue
		}

		fmt.Println("  (waiting — messages may sit in a debounce window)")
		printRecords(pollReplies(base))
	}
}

func send(base string, req injectRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := http.Post(base+"/inject", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, string(b))
	}
	return nil
}

func pollReplies(base string) []sandboxRecord {
	var all []sandboxRecord
	deadline := time.Now().Add(9 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(400 * time.Millisecond)
		resp, err := http.Get(base + "/replies")
		if err != nil {
			continue
		}
		var batch []sandboxRecord
		json.NewDecoder(resp.Body).Decode(&batch)
		resp.Body.Close()
		if len(batch) > 0 {
			all = append(all, batch...)
			deadline = time.Now().Add(800 * time.Millisecond)
		}
	}
	return all
}

func printRecords(records []sandboxRecord) {
	if len(records) == 0 {
		fmt.Println("  (no answer captured yet)")
		return
	}
	for _, rec := range records {
		instr := rec.Instruction
		fmt.Printf("bot> [%s/%s] action=%s intent=%s (ticket=%s notif=%s)\n",
			instr.Platform, instr.SubtypeID, instr.Action, instr.Intent, instr.TicketID, instr.NotificationID)
		for _, s := range instr.Steps {
			switch {
			case s.Value != "":
				fmt.Printf("      %s\n", s.Value)
			case s.Description != "":
				fmt.Printf("      (%s: %s)\n", s.Type, s.Description)
			}
		}
		if rec.RawResult != nil {
			rawJSON, _ := json.MarshalIndent(rec.RawResult, "      ", "  ")
			fmt.Printf("      [raw] ProcessResult:\n%s\n", rawJSON)
		}
	}
}
