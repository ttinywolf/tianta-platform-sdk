package sqliteipc;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * TIANTA high-level task objects (Java port of python/tianta_tasks.py).
 *
 * Four business-specific task types with structured payload + subscribe/on +
 * blocking wait, plus open-ended extension fields:
 *
 *   TmRecordDone / CmdFileDone / DownloadReq / SystemNotice
 *
 * Extension fields are any keys beyond the predefined FIELDS; they are stored
 * at the top level of the JSON payload and preserved on round-trip. Use
 * TaskEvent.metadata() to read only the custom (non-predefined) fields and
 * TaskEvent.addMetadata(k, v) to append one.
 */
public final class TiantaTasks {

    private TiantaTasks() {}

    // ------------------------------------------------------------------
    // TaskEvent (dict + metadata helpers); payload is a Map<String,Object>
    // ------------------------------------------------------------------
    public static class TaskEvent {
        final Map<String, Object> data;
        final Set<String> predefined;

        TaskEvent(Map<String, Object> data, Set<String> predefined) {
            this.data = data;
            this.predefined = predefined;
        }

        public Object get(String key) { return data.get(key); }
        public String getString(String key) {
            Object v = data.get(key);
            return v != null ? String.valueOf(v) : "";
        }
        public long getLong(String key) {
            Object v = data.get(key);
            if (v instanceof Number) return ((Number) v).longValue();
            try { return v != null ? Long.parseLong(v.toString()) : 0; } catch (NumberFormatException e) { return 0; }
        }
        public TaskEvent addMetadata(String key, Object value) {
            if (!"_ts".equals(key) && !"_predefined".equals(key)) {
                predefined.remove(key);
                data.put(key, value);
            }
            return this;
        }
        public Map<String, Object> metadata() {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : data.entrySet()) {
                if (!predefined.contains(e.getKey())) out.put(e.getKey(), e.getValue());
            }
            return out;
        }
    }

    /** One business task type. */
    public static class TaskSpec {
        final String key;
        final Map<String, Object> defs;

        TaskSpec(String key, Map<String, Object> defs) {
            this.key = key;
            this.defs = defs;
        }

        Set<String> predefinedSet() {
            Set<String> s = new java.util.HashSet<>();
            s.add("_ts");
            s.add("_predefined");
            s.addAll(defs.keySet());
            return s;
        }

        public long post(TiHandle h, Map<String, Object> fields) throws Exception {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("_ts", System.currentTimeMillis());
            payload.putAll(defs);
            if (fields != null) payload.putAll(fields);
            return Tianta.TI_task_post(h, key, TiantaTasks.encode(payload));
        }

        public TaskEvent wait(TiHandle h, int timeoutMs) throws Exception {
            String raw = Tianta.TI_task_wait(h, key, timeoutMs);
            return raw == null ? null : parse(raw);
        }

        public long on(TiHandle h, Consumer<TaskEvent> cb) {
            return Tianta.TI_task_subscribe(h, key, raw -> {
                TaskEvent ev = parse(raw);
                if (ev != null) cb.accept(ev);
            });
        }

        public void off(TiHandle h, long tag) {
            Tianta.TI_task_unsubscribe(h, key, tag);
        }

        TaskEvent parse(String raw) {
            Object root = TiantaTasks.decode(raw);
            if (root instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) root;
                return new TaskEvent(m, predefinedSet());
            }
            return null;
        }
    }

    // ------------------------------------------------------------------
    // The four task objects
    // ------------------------------------------------------------------
    public static final TaskSpec TmRecordDone = new TaskSpec(Tianta.TI_KEY_TM_DONE, m(
        kv("file_path", ""), kv("size", 0L), kv("start_utc", ""), kv("end_utc", ""), kv("station", "")));

    public static final TaskSpec CmdFileDone = new TaskSpec(Tianta.TI_KEY_CMD_DONE, m(
        kv("cmd_id", ""), kv("file_path", ""), kv("cmd_type", ""), kv("priority", 5L)));

    public static final TaskSpec DownloadReq = new TaskSpec(Tianta.TI_KEY_DL_REQ, m(
        kv("req_id", ""), kv("data_type", ""), kv("target", ""), kv("priority", 5L)));

    public static final TaskSpec SystemNotice = new TaskSpec(Tianta.TI_KEY_SYS_NTF, m(
        kv("level", "info"), kv("title", ""), kv("body", ""), kv("code", "")));

    // ------------------------------------------------------------------
    // Minimal JSON encode/decode (objects, strings, numbers, bool, null)
    // ------------------------------------------------------------------
    static Map<String, Object> m(Object... kv) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Object o : kv) {
            if (o instanceof Object[] && ((Object[]) o).length == 2) {
                Object[] pair = (Object[]) o;
                map.put(String.valueOf(pair[0]), pair[1]);
            }
        }
        return map;
    }
    static Object[] kv(String k, Object v) { return new Object[]{k, v}; }

    public static String encode(Object o) {
        StringBuilder sb = new StringBuilder();
        encodeValue(o, sb);
        return sb.toString();
    }

    private static void encodeValue(Object o, StringBuilder sb) {
        if (o == null) { sb.append("null"); return; }
        if (o instanceof String) { encodeString((String) o, sb); return; }
        if (o instanceof Boolean) { sb.append(o); return; }
        if (o instanceof Number) { sb.append(o); return; }
        if (o instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                if (!first) sb.append(',');
                first = false;
                encodeString(String.valueOf(e.getKey()), sb);
                sb.append(':');
                encodeValue(e.getValue(), sb);
            }
            sb.append('}');
            return;
        }
        if (o instanceof Iterable) {
            sb.append('[');
            boolean first = true;
            for (Object v : (Iterable<?>) o) {
                if (!first) sb.append(',');
                first = false;
                encodeValue(v, sb);
            }
            sb.append(']');
            return;
        }
        encodeString(String.valueOf(o), sb);
    }

    private static void encodeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') { sb.append('\\'); sb.append(c); }
            else if (c == '\n') sb.append("\\n");
            else if (c == '\r') sb.append("\\r");
            else if (c == '\t') sb.append("\\t");
            else sb.append(c);
        }
        sb.append('"');
    }

    public static Object decode(String s) {
        int[] pos = new int[]{0};
        return decodeValue(s, pos);
    }

    private static Object decodeValue(String s, int[] pos) {
        skipWs(s, pos);
        if (pos[0] >= s.length()) return null;
        char c = s.charAt(pos[0]);
        if (c == '{') return decodeObject(s, pos);
        if (c == '[') return decodeArray(s, pos);
        if (c == '"') return decodeString(s, pos);
        // number / true / false / null
        int start = pos[0];
        while (pos[0] < s.length() && s.charAt(pos[0]) != ',' && s.charAt(pos[0]) != '}' && s.charAt(pos[0]) != ']') {
            pos[0]++;
        }
        String tok = s.substring(start, pos[0]).trim();
        if ("true".equals(tok)) return Boolean.TRUE;
        if ("false".equals(tok)) return Boolean.FALSE;
        if ("null".equals(tok)) return null;
        try { return Long.parseLong(tok); } catch (NumberFormatException e) { /* fall through */ }
        try { return Double.parseDouble(tok); } catch (NumberFormatException e) { return tok; }
    }

    private static Map<String, Object> decodeObject(String s, int[] pos) {
        Map<String, Object> map = new LinkedHashMap<>();
        pos[0]++; // {
        skipWs(s, pos);
        if (pos[0] < s.length() && s.charAt(pos[0]) == '}') { pos[0]++; return map; }
        while (true) {
            skipWs(s, pos);
            String key = decodeString(s, pos);
            skipWs(s, pos);
            pos[0]++; // :
            Object val = decodeValue(s, pos);
            map.put(key, val);
            skipWs(s, pos);
            if (pos[0] < s.length() && s.charAt(pos[0]) == ',') { pos[0]++; continue; }
            if (pos[0] < s.length() && s.charAt(pos[0]) == '}') { pos[0]++; break; }
            break;
        }
        return map;
    }

    private static java.util.List<Object> decodeArray(String s, int[] pos) {
        java.util.List<Object> list = new java.util.ArrayList<>();
        pos[0]++; // [
        skipWs(s, pos);
        if (pos[0] < s.length() && s.charAt(pos[0]) == ']') { pos[0]++; return list; }
        while (true) {
            list.add(decodeValue(s, pos));
            skipWs(s, pos);
            if (pos[0] < s.length() && s.charAt(pos[0]) == ',') { pos[0]++; continue; }
            if (pos[0] < s.length() && s.charAt(pos[0]) == ']') { pos[0]++; break; }
            break;
        }
        return list;
    }

    private static String decodeString(String s, int[] pos) {
        pos[0]++; // opening quote
        StringBuilder sb = new StringBuilder();
        while (pos[0] < s.length()) {
            char c = s.charAt(pos[0]++);
            if (c == '"') break;
            if (c == '\\' && pos[0] < s.length()) {
                char e = s.charAt(pos[0]++);
                if (e == 'n') sb.append('\n');
                else if (e == 'r') sb.append('\r');
                else if (e == 't') sb.append('\t');
                else sb.append(e);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void skipWs(String s, int[] pos) {
        while (pos[0] < s.length() && Character.isWhitespace(s.charAt(pos[0]))) pos[0]++;
    }
}
