// TIANTA high-level task objects (Go port of python/tianta_tasks.py).
//
// Four business-specific task types with structured payload + subscribe/on +
// blocking wait, plus open-ended extension fields:
//
//   TmRecordDone / CmdFileDone / DownloadReq / SystemNotice
//
// Extension fields are any keys beyond the predefined FIELDS; they are stored
// at the top level of the JSON payload and preserved on round-trip. Use
// TaskEvent.Metadata() to read only the custom (non-predefined) fields and
// TaskEvent.AddMetadata(k, v) to append one.
package sqliteipc

import (
	"encoding/json"
)

// TaskEvent mirrors python TaskEvent: JSON top-level map + metadata helpers.
type TaskEvent struct {
	Data       map[string]interface{}
	predefined map[string]bool
}

// Get returns a field by name (any field), or nil.
func (e *TaskEvent) Get(key string) interface{} {
	return e.Data[key]
}

// GetString returns a string field.
func (e *TaskEvent) GetString(key string) string {
	if v, ok := e.Data[key].(string); ok {
		return v
	}
	return ""
}

// GetInt64 returns an int field (0 if absent/not numeric).
func (e *TaskEvent) GetInt64(key string) int64 {
	switch v := e.Data[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		i, _ := v.Int64()
		return i
	}
	return 0
}

// AddMetadata appends/overwrites a custom extension field.
func (e *TaskEvent) AddMetadata(key string, value interface{}) *TaskEvent {
	if key == "_ts" || key == "_predefined" {
		return e
	}
	delete(e.predefined, key)
	e.Data[key] = value
	return e
}

// Metadata returns only the custom (non-predefined) fields.
func (e *TaskEvent) Metadata() map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range e.Data {
		if !e.predefined[k] {
			out[k] = v
		}
	}
	return out
}

// TaskSpec is one business task type.
type TaskSpec struct {
	Key    string
	fields []string            // predefined field names (in order)
	defs   map[string]interface{} // predefined field defaults
}

func newTaskSpec(key string, defs map[string]interface{}) *TaskSpec {
	keys := make([]string, 0, len(defs))
	for k := range defs {
		keys = append(keys, k)
	}
	return &TaskSpec{Key: key, fields: keys, defs: defs}
}

func (s *TaskSpec) predefinedSet() map[string]bool {
	m := map[string]bool{"_ts": true, "_predefined": true}
	for _, k := range s.fields {
		m[k] = true
	}
	return m
}

// Post publishes a structured (or custom) task; returns its id.
func (s *TaskSpec) Post(ch *TiHandle, fields map[string]interface{}) (int64, error) {
	payload := map[string]interface{}{"_ts": tiNowMs()}
	for k, v := range s.defs {
		payload[k] = v
	}
	for k, v := range fields {
		payload[k] = v
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	return TITaskPost(ch, s.Key, string(b))
}

// Wait blocks for one task event (or timeout). Returns nil on timeout/empty.
func (s *TaskSpec) Wait(ch *TiHandle, timeoutMs int) *TaskEvent {
	raw, found, err := TITaskWait(ch, s.Key, timeoutMs)
	if err != nil || !found {
		return nil
	}
	return s.parse(raw)
}

// On subscribes to this task; callback receives TaskEvent per message.
func (s *TaskSpec) On(ch *TiHandle, cb func(ev *TaskEvent)) int64 {
	spec := s
	return TITaskSubscribe(ch, s.Key, func(raw string) {
		if ev := spec.parse(raw); ev != nil {
			cb(ev)
		}
	})
}

// Off unsubscribes.
func (s *TaskSpec) Off(ch *TiHandle, tag int64) {
	TITaskUnsubscribe(ch, s.Key, tag)
}

func (s *TaskSpec) parse(raw string) *TaskEvent {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil
	}
	return &TaskEvent{Data: data, predefined: s.predefinedSet()}
}

// ---------------------------------------------------------------------------
// The four task objects
// ---------------------------------------------------------------------------
var TmRecordDone = newTaskSpec(TIKeyTMDone, map[string]interface{}{
	"file_path": "",
	"size":     0,
	"start_utc": "",
	"end_utc":  "",
	"station":  "",
})

var CmdFileDone = newTaskSpec(TIKeyCmdDone, map[string]interface{}{
	"cmd_id":    "",
	"file_path": "",
	"cmd_type":  "",
	"priority":  5,
})

var DownloadReq = newTaskSpec(TIKeyDLReq, map[string]interface{}{
	"req_id":    "",
	"data_type": "",
	"target":    "",
	"priority":  5,
})

var SystemNotice = newTaskSpec(TIKeySysNtf, map[string]interface{}{
	"level": "info",
	"title": "",
	"body":  "",
	"code":  "",
})
