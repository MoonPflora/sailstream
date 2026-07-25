package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"sailstream/internal/listener"
	"sailstream/internal/nnlp"
	"sailstream/internal/shared"
)

var Enabled bool

var debugHTML = []byte(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sandbox Debugger</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 1rem; min-height: 100vh; }
h1 { color: #4a90d9; margin-bottom: 1rem; font-size: 1.3rem; }
.controls { display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap; align-items: center; }
.controls input, .controls select { padding: 0.5rem; border: 1px solid #333; border-radius: 6px; background: #16213e; color: #e0e0e0; }
.controls button { padding: 0.5rem 1.2rem; background: #4a90d9; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600; }
.controls button:hover { background: #5aa0e9; }
.card { background: #16213e; border-radius: 10px; margin-bottom: 1rem; overflow: hidden; border: 1px solid #0f3460; }
.card-header { padding: 0.75rem 1rem; background: #0f3460; display: flex; justify-content: space-between; align-items: center; cursor: pointer; user-select: none; }
.card-header:hover { background: #1a4080; }
.card-header .user-msg { font-weight: 600; max-width: 60%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #e0e0e0; }
.card-header .meta { color: #8892b0; font-size: 0.75rem; }
.card-body { display: none; }
.card-body.open { display: block; }
.stage { border-bottom: 1px solid #0f3460; }
.stage:last-child { border-bottom: none; }
.stage-header { padding: 0.5rem 1rem; background: #1a1a2e; cursor: pointer; user-select: none; display: flex; align-items: center; gap: 0.5rem; font-size: 0.85rem; color: #a0b0d0; }
.stage-header:hover { background: #16213e; }
.stage-header .arrow { display: inline-block; width: 12px; transition: transform 0.2s; font-size: 0.7rem; }
.stage-header.open .arrow { transform: rotate(90deg); }
.stage-body { display: none; padding: 0.75rem 1rem; background: #0a0a1a; font-size: 0.8rem; }
.stage-body.open { display: block; }
.compiled-text { color: #50fa7b; margin-bottom: 0.5rem; font-style: italic; padding: 0.5rem; background: #0f3460; border-radius: 4px; }
.json-toggle { display: inline-block; margin-right: 0.5rem; font-size: 0.7rem; color: #4a90d9; cursor: pointer; padding: 2px 8px; border: 1px solid #4a90d9; border-radius: 3px; }
.json-toggle:hover { background: #4a90d9; color: white; }
.json-data { display: none; font-family: 'Courier New', monospace; white-space: pre-wrap; background: #000; padding: 0.5rem; border-radius: 4px; margin-top: 0.3rem; font-size: 0.75rem; color: #f8f8f2; overflow-x: auto; }
.json-data.visible { display: block; }
.diff-new { color: #50fa7b; }
.diff-old { color: #ff6b6b; }
.stage-badge { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; font-weight: 600; }
.badge-received { background: #1b4332; color: #95d5b2; }
.badge-nlp_result { background: #1a3a5c; color: #82aaff; }
.badge-compiled { background: #3d2a5c; color: #c792ea; }
.badge-compile_failed { background: #5c1a1a; color: #ff6b6b; }
.badge-delivered { background: #2d5c1a; color: #8ce071; }
.badge-delivery_failed { background: #5c2a1a; color: #ffaa6b; }
.badge-raw_result { background: #2a2a5c; color: #b0b0e0; }
.badge-db_write { background: #4a3300; color: #ffcc66; }
</style>
</head>
<body>
<h1>🔬 Sandbox Debugger</h1>
<div class="controls">
  <input id="text" type="text" placeholder="Type a message..." size="40" autofocus onkeydown="if(event.key==='Enter')send()">
  <select id="platform"><option value="whatsapp">WhatsApp</option><option value="telegram">Telegram</option></select>
  <select id="subtype"><option value="sandbox">sandbox (isolated)</option></select>
  <input id="user" type="text" value="sandbox-user-1" placeholder="User ID" size="12">
  <button id="sendBtn">Send</button>
</div>
<div id="cards"></div>
<script>
(function() {
  "use strict";
  var knownCards = {};
  function escapeHtml(text) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(text)));
    return div.innerHTML;
  }
  // pick tries several possible key spellings for the same field and
  // returns the first defined/non-empty one. Used because we're not 100%
  // certain whether shared.AutomationInstruction serializes fields as
  // PascalCase (Go field names) or snake_case (idiomatic JSON) — guessing
  // wrong silently breaks rendering instead of erroring, so we hedge.
  function pick(obj) {
    if (!obj) return undefined;
    for (var i = 1; i < arguments.length; i++) {
      var v = obj[arguments[i]];
      if (v !== undefined && v !== null && v !== '') return v;
    }
    return undefined;
  }
  // colorizeJSON renders text (a JSON.stringify'd blob) as HTML with each
  // line wrapped in a span: green if this exact line (trimmed) hasn't been
  // seen earlier in this card's trace flow, red if it's an exact repeat of
  // something already shown (i.e. carried over unchanged). seenLines is a
  // Set shared across every stage of one card, in pipeline order, so it
  // accumulates as we walk received -> nlp_result -> db_write -> compiled -> ...
  function colorizeJSON(text, seenLines) {
    return text.split('\n').map(function(line) {
      var key = line.trim();
      var isNew = key !== '' && !seenLines.has(key);
      if (key !== '') seenLines.add(key);
      var cls = isNew ? 'diff-new' : 'diff-old';
      return '<span class="' + cls + '">' + escapeHtml(line) + '</span>';
    }).join('\n');
  }
  function badgeClass(name) {
    var map = {
      'received': 'badge-received',
      'nlp_result': 'badge-nlp_result',
      'compiled': 'badge-compiled',
      'compile_failed': 'badge-compile_failed',
      'delivered': 'badge-delivered',
      'delivery_failed': 'badge-delivery_failed',
      'raw_result': 'badge-raw_result',
      'db_write': 'badge-db_write'
    };
    return map[name] || '';
  }
  function createStage(stageName, events, seenLines) {
    var stage = document.createElement('div');
    stage.className = 'stage';
    var header = document.createElement('div');
    header.className = 'stage-header';
    var countBadge = events.length > 1 ? ' <span class="meta">&times;' + events.length + '</span>' : '';
    header.innerHTML = '<span class="arrow">▶</span> <span class="stage-badge ' + badgeClass(stageName) + '">' + escapeHtml(stageName) + '</span>' + countBadge;
    header.addEventListener('click', function(e) {
      e.stopPropagation();
      header.classList.toggle('open');
      var body = stage.querySelector('.stage-body');
      if (body) body.classList.toggle('open');
    });
    stage.appendChild(header);
    var body = document.createElement('div');
    body.className = 'stage-body';
    events.forEach(function(evt) {
      var wrapper = document.createElement('div');
      wrapper.style.marginBottom = '0.5rem';
      var d = evt.data || {};
      if (d.table) {
        var label = document.createElement('span');
        label.className = 'meta';
        label.style.marginRight = '0.5rem';
        label.textContent = d.table + (d.op ? ' \u00b7 ' + d.op : '');
        wrapper.appendChild(label);
      }
      var toggle = document.createElement('span');
      toggle.className = 'json-toggle';
      toggle.textContent = 'Show JSON';
      wrapper.appendChild(toggle);
      var dataDiv = document.createElement('div');
      dataDiv.className = 'json-data';
      try {
        var jsonText = JSON.stringify(evt.data || evt, null, 2);
        dataDiv.innerHTML = colorizeJSON(jsonText, seenLines);
      } catch (e) {
        dataDiv.textContent = '[Unable to stringify]';
      }
      wrapper.appendChild(dataDiv);
      toggle.addEventListener('click', function(e) {
        e.stopPropagation();
        if (dataDiv.classList.contains('visible')) {
          dataDiv.classList.remove('visible');
          toggle.textContent = 'Show JSON';
        } else {
          dataDiv.classList.add('visible');
          toggle.textContent = 'Hide JSON';
        }
      });
      body.appendChild(wrapper);
    });
    stage.appendChild(body);
    return stage;
  }
  function createCompiledStage(steps) {
    var stage = document.createElement('div');
    stage.className = 'stage';
    var header = document.createElement('div');
    header.className = 'stage-header';
    header.innerHTML = '<span class="arrow">▶</span> <span class="stage-badge badge-compiled">compiled_message</span>';
    header.addEventListener('click', function(e) {
      e.stopPropagation();
      header.classList.toggle('open');
      var body = stage.querySelector('.stage-body');
      if (body) body.classList.toggle('open');
    });
    stage.appendChild(header);
    var body = document.createElement('div');
    body.className = 'stage-body';
    var div = document.createElement('div');
    div.className = 'compiled-text';
    div.textContent = steps.map(function(s) {
      var value = pick(s, 'Value', 'value');
      var type = pick(s, 'Type', 'type');
      var desc = pick(s, 'Description', 'description');
      return value || '(' + type + ': ' + desc + ')';
    }).join(' | ');
    body.appendChild(div);
    stage.appendChild(body);
    return stage;
  }
  var openCard = null;
  function openOnly(card) {
    if (openCard && openCard !== card) {
      var oldBody = openCard.querySelector('.card-body');
      if (oldBody) oldBody.classList.remove('open');
    }
    openCard = card;
    var body = card.querySelector('.card-body');
    if (body) body.classList.add('open');
  }
  function render(records) {
    var container = document.getElementById('cards');
    if (!container) return;
    records.forEach(function(rec) {
      try {
        renderOne(rec, container);
      } catch (e) {
        console.log('Failed to render record:', e, rec);
      }
    });
  }
  function renderOne(rec, container) {
      var instr = rec.instruction || {};
      var raw = rec.raw_result;
      var trace = rec.trace || [];
      // trace[].notification_id is guaranteed lowercase (see TraceEvent in
      // sandbox.go) — use it as the primary id so a possible casing
      // mismatch on the instruction object can't cause every record to
      // collide under the same (undefined) key and silently stop new
      // cards from ever appearing.
      var notifID = (trace[0] && trace[0].notification_id) ||
                    pick(instr, 'NotificationID', 'notification_id') ||
                    (raw && raw.notification_id) ||
                    ('unknown-' + Date.now() + '-' + Math.random());
      if (knownCards[notifID]) {
        console.log('[render] SKIPPING duplicate notifID=' + notifID + ' — a card for this id already exists. If this id repeats across genuinely different messages, that is the bug.');
        return;
      }
      knownCards[notifID] = true;
      var card = document.createElement('div');
      card.className = 'card';
      var header = document.createElement('div');
      header.className = 'card-header';
      var originalText = pick(instr, 'OriginalText', 'original_text');
      var action = pick(instr, 'Action', 'action');
      var platform = pick(instr, 'Platform', 'platform') || (raw && raw.platform) || '';
      var subtype = pick(instr, 'SubtypeID', 'subtype_id') || '';
      var msgText = originalText || (raw && raw.raw_text) || action || '(no text)';
      header.innerHTML = '<span class="user-msg">' + escapeHtml(msgText) + '</span>' +
                         '<span class="meta">' + escapeHtml(platform + '/' + subtype) + '</span>';
      header.addEventListener('click', function() {
        var body = card.querySelector('.card-body');
        if (body.classList.contains('open')) {
          body.classList.remove('open');
          if (openCard === card) openCard = null;
        } else {
          openOnly(card);
        }
      });
      card.appendChild(header);
      var body = document.createElement('div');
      body.className = 'card-body';
      card.appendChild(body);
      container.insertBefore(card, container.firstChild);
      if (!trace || trace.length === 0) {
        body.innerHTML = '<div style="padding:1rem;color:#8892b0;">No trace events for this message.</div>';
      } else {
        var stages = {};
        trace.forEach(function(e) {
          if (!stages[e.stage]) stages[e.stage] = [];
          stages[e.stage].push(e);
        });
        var seenLines = new Set();
        var order = ['received', 'nlp_result', 'db_write', 'compiled', 'compile_failed', 'delivered', 'delivery_failed'];
        order.forEach(function(stageName) {
          if (stages[stageName]) {
            body.appendChild(createStage(stageName, stages[stageName], seenLines));
          }
        });
        var steps = pick(instr, 'Steps', 'steps');
        if (steps && Array.isArray(steps) && steps.length) {
          body.appendChild(createCompiledStage(steps));
        }
        if (raw && Object.keys(raw).length > 0) {
          body.appendChild(createStage('raw_result', [{stage: 'raw_result', data: raw}], seenLines));
        }
      }
      // Newest message always takes focus, collapsing whatever was open
      // before it — keeps exactly one trace visible at a time instead of
      // every card piling up open simultaneously.
      openOnly(card);
  }
  function poll() {
    fetch('/replies?_=' + Date.now(), { cache: 'no-store' })
      .then(function(r) { return r.json(); })
      .then(function(records) {
        pollCount++;
        var n = records ? records.length : 0;
        if (n > 0) {
          console.log('[poll #' + pollCount + '] got ' + n + ' record(s), known cards so far: ' + Object.keys(knownCards).length);
          render(records);
        }
      })
      .catch(function(e) {
        console.log('[poll #' + pollCount + '] error:', e);
      });
    setTimeout(poll, 800);
  }
  var pollCount = 0;
  window.send = function() {
    var textInput = document.getElementById('text');
    var text = textInput.value.trim();
    if (!text) return;
    var platform = document.getElementById('platform').value;
    var subtype = document.getElementById('subtype').value;
    var user = document.getElementById('user').value;
    fetch('/inject', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        platform: platform,
        subtype_id: subtype,
        user_id: user,
        text: text
      })
    }).then(function() {
      textInput.value = '';
    }).catch(function(e) {
      console.error('Inject error:', e);
    });
  };
  document.addEventListener('DOMContentLoaded', function() {
    var btn = document.getElementById('sendBtn');
    if (btn) btn.addEventListener('click', send);
    poll();
  });
})();
</script>
</body>
</html>`)

type TraceEvent struct {
	NotificationID string                 `json:"notification_id"`
	Stage          string                 `json:"stage"`
	Timestamp      time.Time              `json:"timestamp"`
	Data           map[string]interface{} `json:"data,omitempty"`
}

type traceStore struct {
	mu     sync.Mutex
	events []TraceEvent
}

var globalTraceStore = &traceStore{}

func RecordTrace(notificationID, stage string, data map[string]interface{}) {
	if notificationID == "" {
		return
	}
	globalTraceStore.mu.Lock()
	globalTraceStore.events = append(globalTraceStore.events, TraceEvent{
		NotificationID: notificationID,
		Stage:          stage,
		Timestamp:      time.Now().UTC(),
		Data:           data,
	})
	globalTraceStore.mu.Unlock()
}

func GetTrace(notificationID string) []TraceEvent {
	globalTraceStore.mu.Lock()
	defer globalTraceStore.mu.Unlock()
	var out []TraceEvent
	for i := len(globalTraceStore.events) - 1; i >= 0; i-- {
		e := globalTraceStore.events[i]
		if e.NotificationID == notificationID {
			out = append(out, e)
		}
	}
	return out
}

type SandboxRecord struct {
	Instruction *shared.AutomationInstruction `json:"instruction"`
	RawResult   map[string]interface{}        `json:"raw_result,omitempty"`
	Trace       []TraceEvent                  `json:"trace,omitempty"`
}

type AnswerLog struct {
	mu      sync.Mutex
	records []SandboxRecord
}

func NewAnswerLog() *AnswerLog {
	return &AnswerLog{}
}

func (a *AnswerLog) Record(result *nnlp.ProcessResult, instruction *shared.AutomationInstruction) {
	notifID, _ := result.Data["notification_id"].(string)

	if instruction == nil {
		instruction = &shared.AutomationInstruction{
			NotificationID: notifID,
			Action:         "compile_failed",
			Intent:         result.Intent,
			Platform:       fmt.Sprintf("%v", result.Data["platform"]),
			SubtypeID:      fmt.Sprintf("%v", result.Data["notification_type"]),
			TicketID:       result.TicketID,
			OriginalText:   fmt.Sprintf("%v", result.Data["raw_text"]),
		}
	} else {
		RecordTrace(notifID, "compiled", map[string]interface{}{
			"action": instruction.Action,
			"intent": instruction.Intent,
			"ticket": instruction.TicketID,
		})
	}

	trace := GetTrace(notifID)
	sort.Slice(trace, func(i, j int) bool {
		return trace[i].Timestamp.Before(trace[j].Timestamp)
	})

	a.mu.Lock()
	a.records = append(a.records, SandboxRecord{
		Instruction: instruction,
		RawResult:   result.Data,
		Trace:       trace,
	})
	a.mu.Unlock()

	log.Printf("[Sandbox] answer: ticket=%s notif=%s platform=%s/%s action=%s intent=%s",
		instruction.TicketID, instruction.NotificationID,
		instruction.Platform, instruction.SubtypeID, instruction.Action, instruction.Intent)
	for _, step := range instruction.Steps {
		if step.Value != "" {
			log.Printf("[Sandbox]   -> %s: %q", step.Type, step.Value)
		}
	}
}

func (a *AnswerLog) Drain() []SandboxRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.records
	a.records = nil
	return out
}

type Collector struct {
	errCh chan *listener.PlatformError
}

func NewCollector() *Collector {
	return &Collector{errCh: make(chan *listener.PlatformError)}
}

func (c *Collector) ReceiveInstructions(instruction *shared.AutomationInstruction) error {
	log.Printf("[Sandbox] fake collector received ticket=%s action=%s (real send suppressed)",
		instruction.TicketID, instruction.Action)
	return nil
}

func (c *Collector) Collect(ctx context.Context, cookies []*listener.CookieData) ([]*listener.Notification, error) {
	return nil, nil
}

func (c *Collector) GetErrorChannel() <-chan *listener.PlatformError {
	return c.errCh
}

type InjectRequest struct {
	Platform    string `json:"platform"`
	SubtypeID   string `json:"subtype_id"`
	AccountID   string `json:"account_id"`
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Text        string `json:"text"`
	ChatJID     string `json:"chat_jid"`
	ImagePath   string `json:"image_path"`
}

func BuildNotification(req InjectRequest) *listener.Notification {
	if req.SubtypeID == "" {
		req.SubtypeID = "sandbox"
	}
	if req.AccountID == "" {
		req.AccountID = "sandbox-account"
	}
	if req.UserID == "" {
		req.UserID = "sandbox-user-1"
	}
	now := time.Now()
	var media []listener.MediaAttachment
	if req.ImagePath != "" {
		media = append(media, listener.MediaAttachment{
			ID:        fmt.Sprintf("sandbox-media-%d", now.UnixNano()),
			Type:      "image",
			URL:       "sandbox://local/" + req.ImagePath,
			Thumbnail: req.ImagePath,
		})
	}
	rawData := map[string]interface{}{"sandbox": true}
	chatJID := req.ChatJID
	if chatJID == "" && req.Platform == "whatsapp" {
		chatJID = req.UserID + "@s.whatsapp.net"
	}
	if chatJID != "" {
		rawData["chat_jid"] = chatJID
	}
	return &listener.Notification{
		ID:          fmt.Sprintf("sandbox-%d", now.UnixNano()),
		Type:        listener.NotificationTypeMessage,
		PlatformID:  req.Platform,
		SubtypeID:   req.SubtypeID,
		AccountID:   req.AccountID,
		Timestamp:   now,
		CollectedAt: now,
		RawData:     rawData,
		Message: &listener.MessageData{
			Sender: listener.UserInfo{
				UserID:      req.UserID,
				Username:    req.Username,
				DisplayName: req.DisplayName,
			},
			ConversationID: req.UserID,
			MessageID:      fmt.Sprintf("sandbox-msg-%d", now.UnixNano()),
			Text:           req.Text,
			Timestamp:      now,
			DeliveryStatus: "delivered",
			MediaAttached:  media,
		},
	}
}

type injector interface {
	InjectNotification(n *listener.Notification) error
}

func StartHTTP(m injector, answers *AnswerLog, addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/inject", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req InjectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
			return
		}
		if req.Platform != "whatsapp" && req.Platform != "telegram" {
			http.Error(w, `"platform" must be "whatsapp" or "telegram"`, http.StatusBadRequest)
			return
		}
		notif := BuildNotification(req)
		if err := m.InjectNotification(notif); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":          "injected",
			"notification_id": notif.ID,
		})
	})

	mux.HandleFunc("/replies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		json.NewEncoder(w).Encode(answers.Drain())
	})

	mux.HandleFunc("/trace", func(w http.ResponseWriter, r *http.Request) {
		notifID := r.URL.Query().Get("notif_id")
		if notifID == "" {
			http.Error(w, "notif_id required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(GetTrace(notifID))
	})

	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(debugHTML)
	})

	go func() {
		log.Printf("[Sandbox] test injector listening on http://%s (debug UI at http://%s/debug)", addr, addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[Sandbox] HTTP server error: %v", err)
		}
	}()
}
