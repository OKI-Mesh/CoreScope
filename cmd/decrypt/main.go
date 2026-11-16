// corescope-decrypt decrypts and exports hashtag channel messages from a CoreScope Postgres database.
//
// Usage:
//
//	corescope-decrypt --channel "#wardriving" [--dsn URL] [--format json|html|irc|log] [--output file]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/channel"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Version info (set via ldflags).
var version = "dev"

// pageSize is how many GRP_TXT transmissions are read per query.
const pageSize = 5000

// ChannelMessage is a single decrypted channel message with metadata.
type ChannelMessage struct {
	Hash      string     `json:"hash"`
	Timestamp string     `json:"timestamp"`
	Sender    string     `json:"sender"`
	Message   string     `json:"message"`
	Channel   string     `json:"channel"`
	RawHex    string     `json:"raw_hex"`
	Path      []string   `json:"path"`
	Observers []Observer `json:"observers"`
}

// Observer is a single observation of the transmission.
type Observer struct {
	Name      string  `json:"name"`
	SNR       float64 `json:"snr"`
	RSSI      float64 `json:"rssi"`
	Timestamp string  `json:"timestamp"`
}

func main() {
	channelName := flag.String("channel", "", "Channel name (e.g. \"#wardriving\")")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string (default: $DATABASE_URL)")
	format := flag.String("format", "json", "Output format: json, html, irc (or log)")
	output := flag.String("output", "", "Output file (default: stdout)")
	showVersion := flag.Bool("version", false, "Print version and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `corescope-decrypt — Decrypt and export MeshCore hashtag channel messages

USAGE
  corescope-decrypt --channel NAME [--dsn URL] [--format FORMAT] [--output FILE]

FLAGS
  --channel NAME   Channel name to decrypt (e.g. "#wardriving", "wardriving")
                   The "#" prefix is added automatically if missing.
  --dsn URL        Postgres connection string. Defaults to $DATABASE_URL. The DSN
                   carries a password, so prefer the environment variable: a flag
                   shows up in the process list. Access is read-only.
  --format FORMAT  Output format (default: json):
                     json  — Machine-readable JSON array with full metadata
                     html  — Self-contained HTML viewer with search and sorting
                     irc   — Plain-text IRC-style log, one line per message
                     log   — Alias for irc
  --output FILE    Write output to FILE instead of stdout.
  --version        Print version and exit.

EXAMPLES
  # Export #wardriving messages as JSON
  corescope-decrypt --channel "#wardriving"

  # Generate an interactive HTML viewer
  corescope-decrypt --channel wardriving --format html --output wardriving.html

  # Greppable IRC log
  corescope-decrypt --channel "#MeshCore" --format irc --output meshcore.log
  grep "KE6QR" meshcore.log

  # From the Docker container
  docker exec corescope-prod /app/corescope-decrypt --channel "#wardriving"

RETROACTIVE DECRYPTION
  MeshCore hashtag channels use symmetric encryption — the key is derived from the
  channel name. The CoreScope ingestor stores ALL GRP_TXT packets in the database,
  even those it cannot decrypt at ingest time. This tool lets you retroactively
  decrypt messages for any channel whose name you know, even if the ingestor was
  never configured with that channel's key.

  This means you can recover historical messages by simply knowing the channel name.
  Messages whose payload has been scrubbed by the retention roll-off cannot be recovered.

LIMITATIONS
  - Only hashtag channels (shared-secret, name-derived key) are supported.
  - Direct messages (TXT_MSG) use per-peer encryption and cannot be decrypted.
  - Custom PSK channels (non-hashtag) require the raw key, not a channel name.
`)
	}

	flag.Parse()

	if *showVersion {
		fmt.Println("corescope-decrypt", version)
		os.Exit(0)
	}

	if *channelName == "" || *dsn == "" {
		flag.Usage()
		os.Exit(1)
	}

	switch *format {
	case "json", "html", "irc", "log":
	default:
		log.Fatalf("Unknown format: %s (use json, html, irc, or log)", *format)
	}

	// Normalize channel name
	ch := *channelName
	if !strings.HasPrefix(ch, "#") {
		ch = "#" + ch
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := run(ctx, *dsn, ch, *format, *output); err != nil {
		stop()
		log.Fatal(err)
	}
	stop()
}

func run(ctx context.Context, dsn, ch, format, output string) error {
	db, err := database.OpenReadOnly(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	messages, total, decrypted, err := exportChannel(ctx, db, ch, pageSize)
	if err != nil {
		return err
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp < messages[j].Timestamp
	})

	log.Printf("Scanned %d GRP_TXT packets, decrypted %d for channel %s", total, decrypted, ch)

	var out []byte
	switch format {
	case "json":
		out, err = json.MarshalIndent(messages, "", "  ")
		if err != nil {
			return fmt.Errorf("JSON marshal: %w", err)
		}
		out = append(out, '\n')
	case "html":
		out = renderHTML(messages, ch)
	default: // irc, log
		out = renderIRC(messages)
	}

	if output != "" {
		if err := os.WriteFile(output, out, 0o644); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
		log.Printf("Written to %s", output)
		return nil
	}
	_, err = os.Stdout.Write(out)
	return err
}

// exportChannel scans every GRP_TXT transmission, page by page, and returns the
// ones that decrypt under ch's key, plus how many transmissions it scanned and
// how many it decrypted.
func exportChannel(ctx context.Context, db *database.DB, ch string, limit int) ([]ChannelMessage, int, int, error) {
	key := channel.DeriveKey(ch)
	chHash := channel.ChannelHash(key)

	messages := make([]ChannelMessage, 0)
	total, decrypted := 0, 0
	var afterID int64

	for {
		page, err := db.ListGroupTextTransmissions(ctx, afterID, limit)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("list GRP_TXT transmissions: %w", err)
		}
		if len(page) == 0 {
			return messages, total, decrypted, nil
		}
		afterID = page[len(page)-1].ID

		var pageMessages []ChannelMessage
		var pageIDs []int64
		for _, tx := range page {
			total++

			path, payload, err := splitGroupText(tx.Frame)
			if err != nil {
				continue
			}
			ts, sender, msg, ok := decryptGroupText(key, chHash, payload)
			if !ok {
				continue
			}
			decrypted++

			pageIDs = append(pageIDs, tx.ID)
			pageMessages = append(pageMessages, ChannelMessage{
				Hash:      tx.Hash,
				Timestamp: time.Unix(int64(ts), 0).UTC().Format(time.RFC3339),
				Sender:    sender,
				Message:   msg,
				Channel:   ch,
				RawHex:    tx.Frame.Hex(),
				Path:      path,
			})
		}

		if len(pageIDs) > 0 {
			receptions, err := db.ListObserverReceptions(ctx, pageIDs)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("list observer receptions: %w", err)
			}
			for i := range pageMessages {
				pageMessages[i].Observers = toObservers(receptions[pageIDs[i]])
			}
		}
		messages = append(messages, pageMessages...)
	}
}

// decryptGroupText tries to decrypt a GRP_TXT payload (channel hash byte, 2-byte
// MAC, ciphertext) under key. ok is false when the payload belongs to another
// channel, is too short, fails the MAC, or does not parse.
func decryptGroupText(key []byte, chHash byte, payload []byte) (ts uint32, sender, message string, ok bool) {
	if len(payload) < 3 || payload[0] != chHash {
		return 0, "", "", false
	}
	mac := payload[1:3]
	ciphertext := payload[3:]
	if len(ciphertext) < 16 {
		return 0, "", "", false
	}
	// Truncate to a block boundary for the decryption attempt.
	ciphertext = ciphertext[:len(ciphertext)/16*16]

	plaintext, ok := channel.Decrypt(key, mac, ciphertext)
	if !ok {
		return 0, "", "", false
	}
	ts, sender, message, err := channel.ParsePlaintext(plaintext)
	if err != nil {
		return 0, "", "", false
	}
	return ts, sender, message, true
}

// splitGroupText parses a GRP_TXT frame and returns its path hops (uppercase
// hex) and its payload bytes.
func splitGroupText(frame types.RawFrame) (path []string, payload []byte, err error) {
	env, err := decoder.ParseEnvelope(frame)
	if err != nil {
		return nil, nil, err
	}
	if env.Header.PayloadType != types.PayloadGRP_TXT {
		return nil, nil, fmt.Errorf("not GRP_TXT")
	}
	return env.Path.Hops, env.Payload, nil
}

func toObservers(receptions []database.ObserverReception) []Observer {
	var observers []Observer
	for _, r := range receptions {
		o := Observer{Timestamp: r.ReceivedAt.UTC().Format(time.RFC3339)}
		if r.ObserverName != nil {
			o.Name = *r.ObserverName
		}
		if r.SNR != nil {
			o.SNR = *r.SNR
		}
		if r.RSSI != nil {
			o.RSSI = *r.RSSI
		}
		observers = append(observers, o)
	}
	return observers
}

func renderIRC(messages []ChannelMessage) []byte {
	var b strings.Builder
	for _, m := range messages {
		sender := m.Sender
		if sender == "" {
			sender = "???"
		}
		// Parse RFC3339 timestamp into a compact format
		t, err := time.Parse(time.RFC3339, m.Timestamp)
		if err != nil {
			b.WriteString(fmt.Sprintf("[%s] <%s> %s\n", m.Timestamp, sender, m.Message))
			continue
		}
		b.WriteString(fmt.Sprintf("[%s] <%s> %s\n", t.Format("2006-01-02 15:04:05"), sender, m.Message))
	}
	return []byte(b.String())
}

func renderHTML(messages []ChannelMessage, channelName string) []byte {
	jsonData, _ := json.Marshal(messages)

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CoreScope Channel Export — ` + html.EscapeString(channelName) + `</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0d1117;color:#c9d1d9;padding:20px}
h1{color:#58a6ff;margin-bottom:16px;font-size:1.5em}
.stats{color:#8b949e;margin-bottom:16px;font-size:0.9em}
input[type=text]{width:100%;max-width:500px;padding:8px 12px;background:#161b22;border:1px solid #30363d;border-radius:6px;color:#c9d1d9;font-size:14px;margin-bottom:16px}
input[type=text]:focus{outline:none;border-color:#58a6ff}
table{width:100%;border-collapse:collapse;font-size:14px}
th{background:#161b22;color:#8b949e;text-align:left;padding:8px 12px;border-bottom:2px solid #30363d;cursor:pointer;user-select:none;white-space:nowrap}
th:hover{color:#58a6ff}
th.sorted-asc::after{content:" ▲"}
th.sorted-desc::after{content:" ▼"}
td{padding:8px 12px;border-bottom:1px solid #21262d;vertical-align:top}
tr:hover{background:#161b22}
tr.expanded{background:#161b22}
.detail-row td{padding:12px 24px;background:#0d1117;border-bottom:1px solid #21262d}
.detail-row pre{background:#161b22;padding:12px;border-radius:6px;overflow-x:auto;font-size:12px;color:#8b949e}
.detail-row .label{color:#58a6ff;font-weight:600;margin-top:8px;display:block}
.observer-tag{display:inline-block;background:#1f6feb22;color:#58a6ff;padding:2px 8px;border-radius:4px;margin:2px;font-size:12px}
.no-results{color:#8b949e;text-align:center;padding:40px;font-size:16px}
.sender{color:#d2a8ff;font-weight:600}
.timestamp{color:#8b949e;font-family:monospace;font-size:12px}
</style>
</head>
<body>
<h1>` + html.EscapeString(channelName) + ` — Channel Messages</h1>
<div class="stats" id="stats"></div>
<input type="text" id="search" placeholder="Search messages..." autocomplete="off">
<table>
<thead>
<tr>
<th data-col="timestamp">Timestamp</th>
<th data-col="sender">Sender</th>
<th data-col="message">Message</th>
<th data-col="observers">Observers</th>
</tr>
</thead>
<tbody id="tbody"></tbody>
</table>
<div class="no-results" id="no-results" style="display:none">No matching messages</div>
<script>
var DATA=` + string(jsonData) + `;
var sortCol="timestamp",sortAsc=true,expandedHash=null;
function init(){
document.getElementById("stats").textContent=DATA.length+" messages";
document.getElementById("search").addEventListener("input",render);
document.querySelectorAll("th[data-col]").forEach(function(th){
th.addEventListener("click",function(){
var col=th.dataset.col;
if(sortCol===col)sortAsc=!sortAsc;
else{sortCol=col;sortAsc=true}
render();
});
});
render();
}
function render(){
var q=document.getElementById("search").value.toLowerCase();
var filtered=DATA.filter(function(m){
if(!q)return true;
return(m.message||"").toLowerCase().indexOf(q)>=0||(m.sender||"").toLowerCase().indexOf(q)>=0;
});
filtered.sort(function(a,b){
var va=a[sortCol]||"",vb=b[sortCol]||"";
if(sortCol==="observers"){va=a.observers?a.observers.length:0;vb=b.observers?b.observers.length:0}
if(va<vb)return sortAsc?-1:1;
if(va>vb)return sortAsc?1:-1;
return 0;
});
document.querySelectorAll("th[data-col]").forEach(function(th){
th.className=th.dataset.col===sortCol?(sortAsc?"sorted-asc":"sorted-desc"):"";
});
var tb=document.getElementById("tbody");
tb.innerHTML="";
document.getElementById("no-results").style.display=filtered.length?"none":"block";
filtered.forEach(function(m){
var tr=document.createElement("tr");
tr.innerHTML='<td class="timestamp">'+esc(m.timestamp)+'</td><td class="sender">'+esc(m.sender||"—")+'</td><td>'+esc(m.message)+'</td><td>'+
(m.observers?m.observers.map(function(o){return'<span class="observer-tag">'+esc(o.name||"?")+" SNR:"+o.snr.toFixed(1)+'</span>'}).join(""):"—")+'</td>';
tr.style.cursor="pointer";
tr.addEventListener("click",function(){
expandedHash=expandedHash===m.hash?null:m.hash;
render();
});
tb.appendChild(tr);
if(expandedHash===m.hash){
tr.className="expanded";
var dr=document.createElement("tr");
dr.className="detail-row";
dr.innerHTML='<td colspan="4"><span class="label">Hash</span><pre>'+esc(m.hash)+'</pre>'+
'<span class="label">Raw Hex</span><pre>'+esc(m.raw_hex)+'</pre>'+
(m.path&&m.path.length?'<span class="label">Path</span><pre>'+esc(m.path.join(" → "))+'</pre>':'')+
'<span class="label">Observers</span><pre>'+esc(JSON.stringify(m.observers,null,2))+'</pre></td>';
tb.appendChild(dr);
}
});
}
function esc(s){var d=document.createElement("div");d.textContent=s;return d.innerHTML}
init();
</script>
</body>
</html>`)

	return []byte(b.String())
}
