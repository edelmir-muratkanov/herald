// Команда providersim — заглушка внешних провайдеров SMS и push для локальной разработки.
//
// Принимает запросы на отправку, хранит их в памяти и умеет имитировать сбои:
// задержку, ответы 500 и 429, зависшие запросы. Режим меняется на лету через
// веб-интерфейс (html/template + htmx) или JSON API (/admin/...).
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed templates static
var assets embed.FS

const (
	maxMessages  = 1000             // сколько последних сообщений хранить
	liveLimit    = 50               // сколько сообщений на канал показывать
	hangDuration = 60 * time.Second // сколько «висит» зависший запрос, если клиент не отключился
	uiTestWait   = 15 * time.Second // сколько тестовая отправка из интерфейса ждёт ответа
)

var channels = []string{"sms", "push"}

// config — режим отказов одного канала.
type config struct {
	LatencyMS    int     `json:"latency_ms"`    // задержка ответа
	FailRate     float64 `json:"fail_rate"`     // доля ответов 500
	ThrottleRate float64 `json:"throttle_rate"` // доля ответов 429 с Retry-After
	HangRate     float64 `json:"hang_rate"`     // доля запросов, на которые ответа не будет
}

func (c config) validate() error {
	if c.LatencyMS < 0 || c.LatencyMS > 60000 {
		return errors.New("задержка должна быть от 0 до 60000 мс")
	}
	rates := []struct {
		name string
		v    float64
	}{{"доля ошибок 500", c.FailRate}, {"доля 429", c.ThrottleRate}, {"доля зависаний", c.HangRate}}
	for _, r := range rates {
		if r.v < 0 || r.v > 1 {
			return fmt.Errorf("%s должна быть от 0 до 100%%", r.name)
		}
	}
	if c.FailRate+c.ThrottleRate+c.HangRate > 1.0000001 {
		return errors.New("сумма долей ошибок, 429 и зависаний не должна превышать 100%")
	}
	return nil
}

type preset struct {
	Name string
	Cfg  config
}

var presets = []preset{
	{"Норма", config{LatencyMS: 50}},
	{"Нестабильный", config{LatencyMS: 200, FailRate: 0.3}},
	{"Лимит запросов", config{LatencyMS: 50, ThrottleRate: 0.6}},
	{"Зависает", config{LatencyMS: 50, HangRate: 0.5}},
	{"Недоступен", config{LatencyMS: 50, FailRate: 1}},
}

type message struct {
	ID             string         `json:"id"`
	Channel        string         `json:"channel"`
	ReceivedAt     time.Time      `json:"received_at"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Payload        map[string]any `json:"payload"`
}

type stats struct {
	Received   int `json:"received"`   // все запросы
	Accepted   int `json:"accepted"`   // приняты (200)
	Duplicates int `json:"duplicates"` // повтор Idempotency-Key
	Rejected   int `json:"rejected"`   // 4xx: невалидные запросы
	Failed     int `json:"failed"`     // 500
	Throttled  int `json:"throttled"`  // 429
	Hung       int `json:"hung"`       // зависшие запросы
}

type sim struct {
	mu      sync.Mutex
	cfg     map[string]config
	stats   map[string]*stats
	msgs    []message         // от старых к новым
	keys    map[string]string // "канал|ключ" -> id сообщения
	seq     int
	mailpit string
	tmpl    *template.Template
}

func newSim(base config, mailpitURL string) *sim {
	s := &sim{
		cfg:     map[string]config{},
		stats:   map[string]*stats{},
		keys:    map[string]string{},
		mailpit: mailpitURL,
		tmpl:    template.Must(template.ParseFS(assets, "templates/*.html")),
	}
	for _, ch := range channels {
		s.cfg[ch] = base
		s.stats[ch] = &stats{}
	}
	return s
}

func (s *sim) config(ch string) config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg[ch]
}

func (s *sim) setConfig(ch string, c config) {
	s.mu.Lock()
	s.cfg[ch] = c
	s.mu.Unlock()
	slog.Info("режим изменён", "channel", ch, "config", c)
}

func (s *sim) bump(ch string, f func(*stats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s.stats[ch])
}

// accept сохраняет сообщение. Повторный Idempotency-Key не создаёт новое сообщение.
func (s *sim) accept(ch, key string, payload map[string]any) (id string, duplicate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if key != "" {
		if id, ok := s.keys[ch+"|"+key]; ok {
			s.stats[ch].Duplicates++
			return id, true
		}
	}
	s.seq++
	id = fmt.Sprintf("%s-%06d", ch, s.seq)
	s.msgs = append(s.msgs, message{
		ID: id, Channel: ch, ReceivedAt: time.Now(), IdempotencyKey: key, Payload: payload,
	})
	if len(s.msgs) > maxMessages {
		s.msgs = s.msgs[len(s.msgs)-maxMessages:]
	}
	if key != "" {
		s.keys[ch+"|"+key] = id
	}
	s.stats[ch].Accepted++
	return id, false
}

func (s *sim) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = nil
	s.keys = map[string]string{}
	for _, ch := range channels {
		s.stats[ch] = &stats{}
	}
}

// validator возвращает HTTP-статус ошибки и сообщение, либо 0, если запрос корректен.
type validator func(p map[string]any) (status int, msg string)

func str(p map[string]any, k string) string {
	s, _ := p[k].(string)
	return strings.TrimSpace(s)
}

// Получатель со словом "invalid" всегда даёт постоянную ошибку: так можно
// детерминированно проверять путь «ошибка без повторов -> DLQ».
func validateSMS(p map[string]any) (int, string) {
	to, text := str(p, "to"), str(p, "text")
	if to == "" || text == "" {
		return http.StatusBadRequest, "поля to и text обязательны"
	}
	if strings.Contains(to, "invalid") {
		return http.StatusUnprocessableEntity, "номер недействителен"
	}
	return 0, ""
}

func validatePush(p map[string]any) (int, string) {
	token := str(p, "token")
	if token == "" || (str(p, "title") == "" && str(p, "body") == "") {
		return http.StatusBadRequest, "нужны token и title или body"
	}
	if strings.Contains(token, "invalid") {
		return http.StatusNotFound, "токен не зарегистрирован"
	}
	return 0, ""
}

var validators = map[string]validator{"sms": validateSMS, "push": validatePush}

// result — итог обработки запроса на отправку. Один и тот же код обслуживает
// и HTTP API провайдера, и тестовую отправку из веб-интерфейса.
type result struct {
	Status     int // 0: клиент отключился или истекло время ожидания
	Body       map[string]any
	RetryAfter string
	Duplicate  bool
}

func errBody(msg string) map[string]any { return map[string]any{"error": msg} }

func (s *sim) process(ctx context.Context, ch string, payload map[string]any, key string) result {
	s.bump(ch, func(st *stats) { st.Received++ })

	if status, msg := validators[ch](payload); status != 0 {
		s.bump(ch, func(st *stats) { st.Rejected++ })
		slog.Info("запрос отклонён", "channel", ch, "status", status)
		return result{Status: status, Body: errBody(msg)}
	}

	cfg := s.config(ch)
	if !sleep(ctx, time.Duration(cfg.LatencyMS)*time.Millisecond) {
		return result{}
	}

	switch roll := rand.Float64(); {
	case roll < cfg.HangRate:
		s.bump(ch, func(st *stats) { st.Hung++ })
		slog.Info("запрос завис", "channel", ch)
		select {
		case <-ctx.Done():
			return result{}
		case <-time.After(hangDuration):
			return result{Status: http.StatusGatewayTimeout, Body: errBody("таймаут провайдера")}
		}
	case roll < cfg.HangRate+cfg.ThrottleRate:
		s.bump(ch, func(st *stats) { st.Throttled++ })
		slog.Info("лимит запросов", "channel", ch)
		return result{Status: http.StatusTooManyRequests, Body: errBody("слишком много запросов"), RetryAfter: "2"}
	case roll < cfg.HangRate+cfg.ThrottleRate+cfg.FailRate:
		s.bump(ch, func(st *stats) { st.Failed++ })
		slog.Info("ошибка провайдера", "channel", ch)
		return result{Status: http.StatusInternalServerError, Body: errBody("внутренняя ошибка провайдера")}
	}

	id, dup := s.accept(ch, key, payload)
	slog.Info("сообщение принято", "channel", ch, "id", id, "duplicate", dup)
	return result{
		Status:    http.StatusOK,
		Body:      map[string]any{"id": id, "status": "accepted", "duplicate": dup},
		Duplicate: dup,
	}
}

// apiSend — HTTP API провайдера (то, что вызывает Worker).
func (s *sim) apiSend(ch string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&payload); err != nil {
			s.bump(ch, func(st *stats) { st.Received++; st.Rejected++ })
			writeJSON(w, http.StatusBadRequest, errBody("невалидный JSON"))
			return
		}
		res := s.process(r.Context(), ch, payload, r.Header.Get("Idempotency-Key"))
		if res.Status == 0 {
			return // клиент отключился
		}
		if res.RetryAfter != "" {
			w.Header().Set("Retry-After", res.RetryAfter)
		}
		writeJSON(w, res.Status, res.Body)
	}
}

// ---- JSON admin API (для скриптов и тестов) ----

type channelState struct {
	Config   config    `json:"config"`
	Stats    stats     `json:"stats"`
	Messages []message `json:"messages"` // новые первыми
}

func (s *sim) snapshot(ch string, limit int) channelState {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs := channelState{Config: s.cfg[ch], Stats: *s.stats[ch], Messages: []message{}}
	for i := len(s.msgs) - 1; i >= 0 && len(cs.Messages) < limit; i-- {
		if s.msgs[i].Channel == ch {
			cs.Messages = append(cs.Messages, s.msgs[i])
		}
	}
	return cs
}

func (s *sim) adminState(w http.ResponseWriter, _ *http.Request) {
	chs := map[string]channelState{}
	for _, ch := range channels {
		chs[ch] = s.snapshot(ch, liveLimit)
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": chs})
}

func (s *sim) adminGetConfig(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	if !validChannel(ch) {
		writeJSON(w, http.StatusNotFound, errBody("неизвестный канал"))
		return
	}
	writeJSON(w, http.StatusOK, s.config(ch))
}

// adminPutConfig меняет только переданные поля.
func (s *sim) adminPutConfig(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	if !validChannel(ch) {
		writeJSON(w, http.StatusNotFound, errBody("неизвестный канал"))
		return
	}
	cfg := s.config(ch)
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("невалидный JSON"))
		return
	}
	if err := cfg.validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
		return
	}
	s.setConfig(ch, cfg)
	writeJSON(w, http.StatusOK, cfg)
}

func (s *sim) adminReset(w http.ResponseWriter, _ *http.Request) {
	s.reset()
	w.WriteHeader(http.StatusNoContent)
}

// ---- Веб-интерфейс: серверный рендеринг, htmx отвечает за частичные обновления ----

type field struct{ Name, Label, Value string }

type configView struct {
	Channel      string
	Latency      int
	Fail         int // проценты
	Throttle     int
	Hang         int
	Presets      []preset
	Message      string
	MessageClass string // ok | bad
}

type channelView struct {
	Channel string
	Title   string
	Fields  []field
	Config  configView
}

type messageRow struct{ Time, ID, Recipient, Text, Key string }

type liveView struct {
	Stats    stats
	Messages []messageRow
}

type resultView struct{ Class, Text string }

var channelDefs = map[string]struct {
	Title  string
	Fields []field
}{
	"sms": {"SMS", []field{
		{"to", "Номер", "+77001234567"},
		{"text", "Текст", "Код подтверждения: 123456"},
	}},
	"push": {"Push", []field{
		{"token", "Токен", "device-token-1"},
		{"title", "Заголовок", "Herald"},
		{"body", "Текст", "Новое уведомление"},
	}},
}

func percent(r float64) int { return int(math.Round(r * 100)) }

func (s *sim) configView(ch, msg, class string) configView {
	c := s.config(ch)
	return configView{
		Channel: ch, Latency: c.LatencyMS,
		Fail: percent(c.FailRate), Throttle: percent(c.ThrottleRate), Hang: percent(c.HangRate),
		Presets: presets, Message: msg, MessageClass: class,
	}
}

func (s *sim) render(w http.ResponseWriter, name string, data any) {
	var buf strings.Builder
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		slog.Error("ошибка шаблона", "template", name, "err", err)
		http.Error(w, "ошибка шаблона", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(buf.String()))
}

func (s *sim) uiPage(w http.ResponseWriter, _ *http.Request) {
	var chs []channelView
	for _, ch := range channels {
		def := channelDefs[ch]
		chs = append(chs, channelView{Channel: ch, Title: def.Title, Fields: def.Fields, Config: s.configView(ch, "", "")})
	}
	s.render(w, "page", map[string]any{"Mailpit": s.mailpit, "Channels": chs})
}

func (s *sim) uiLive(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	if !validChannel(ch) {
		http.NotFound(w, r)
		return
	}
	cs := s.snapshot(ch, liveLimit)
	rows := make([]messageRow, 0, len(cs.Messages))
	for _, m := range cs.Messages {
		row := messageRow{Time: m.ReceivedAt.Format("15:04:05"), ID: m.ID, Key: m.IdempotencyKey}
		if ch == "sms" {
			row.Recipient, row.Text = str(m.Payload, "to"), str(m.Payload, "text")
		} else {
			row.Recipient = str(m.Payload, "token")
			row.Text = strings.Join(nonEmpty(str(m.Payload, "title"), str(m.Payload, "body")), " — ")
		}
		rows = append(rows, row)
	}
	s.render(w, "live", liveView{Stats: cs.Stats, Messages: rows})
}

func nonEmpty(vals ...string) []string {
	var out []string
	for _, v := range vals {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// uiConfig обрабатывает форму режима отказов: либо нажата кнопка пресета, либо «Применить».
func (s *sim) uiConfig(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	if !validChannel(ch) {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.render(w, "config", s.configView(ch, "некорректная форма", "bad"))
		return
	}

	var cfg config
	if name := r.PostForm.Get("preset"); name != "" {
		found := false
		for _, p := range presets {
			if p.Name == name {
				cfg, found = p.Cfg, true
			}
		}
		if !found {
			s.render(w, "config", s.configView(ch, "неизвестный пресет", "bad"))
			return
		}
	} else {
		lat, e1 := strconv.Atoi(r.PostForm.Get("latency"))
		fail, e2 := strconv.Atoi(r.PostForm.Get("fail"))
		thr, e3 := strconv.Atoi(r.PostForm.Get("throttle"))
		hang, e4 := strconv.Atoi(r.PostForm.Get("hang"))
		if err := errors.Join(e1, e2, e3, e4); err != nil {
			s.render(w, "config", s.configView(ch, "значения должны быть числами", "bad"))
			return
		}
		cfg = config{LatencyMS: lat, FailRate: float64(fail) / 100, ThrottleRate: float64(thr) / 100, HangRate: float64(hang) / 100}
	}
	if err := cfg.validate(); err != nil {
		s.render(w, "config", s.configView(ch, err.Error(), "bad"))
		return
	}
	s.setConfig(ch, cfg)
	s.render(w, "config", s.configView(ch, "Применено", "ok"))
}

// uiTest отправляет тестовый запрос через ту же логику, что и HTTP API.
func (s *sim) uiTest(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	if !validChannel(ch) {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.render(w, "result", resultView{"bad", "некорректная форма"})
		return
	}
	payload := map[string]any{}
	for _, f := range channelDefs[ch].Fields {
		payload[f.Name] = r.PostForm.Get(f.Name)
	}

	ctx, cancel := context.WithTimeout(r.Context(), uiTestWait)
	defer cancel()
	start := time.Now()
	res := s.process(ctx, ch, payload, strings.TrimSpace(r.PostForm.Get("key")))
	ms := time.Since(start).Milliseconds()

	switch {
	case res.Status == 0:
		s.render(w, "result", resultView{"warn", fmt.Sprintf("Таймаут клиента (%d с): провайдер завис", int(uiTestWait.Seconds()))})
	case res.Status == http.StatusOK:
		text := fmt.Sprintf("200 · %d мс · id %v", ms, res.Body["id"])
		if res.Duplicate {
			text += " (дубль)"
		}
		s.render(w, "result", resultView{"ok", text})
	default:
		class := "bad"
		if res.Status == http.StatusTooManyRequests || res.Status >= 500 {
			class = "warn"
		}
		s.render(w, "result", resultView{class, fmt.Sprintf("%d · %d мс · %v", res.Status, ms, res.Body["error"])})
	}
}

// uiReset очищает сообщения и просит все панели обновиться.
func (s *sim) uiReset(w http.ResponseWriter, _ *http.Request) {
	s.reset()
	w.Header().Set("HX-Trigger", "refresh")
	w.WriteHeader(http.StatusOK)
}

func validChannel(ch string) bool {
	for _, c := range channels {
		if c == ch {
			return true
		}
	}
	return false
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fatal("неверное значение переменной", "key", key, "value", v)
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		fatal("неверное значение переменной", "key", key, "value", v)
	}
	return f
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	base := config{
		LatencyMS:    envInt("SIM_LATENCY_MS", 50),
		FailRate:     envFloat("SIM_FAIL_RATE", 0),
		ThrottleRate: envFloat("SIM_THROTTLE_RATE", 0),
		HangRate:     envFloat("SIM_HANG_RATE", 0),
	}
	if err := base.validate(); err != nil {
		fatal("неверная начальная конфигурация", "err", err)
	}
	s := newSim(base, env("MAILPIT_URL", "http://localhost:8025"))

	mux := http.NewServeMux()
	// API провайдера
	mux.HandleFunc("POST /sms/send", s.apiSend("sms"))
	mux.HandleFunc("POST /push/send", s.apiSend("push"))
	// JSON API для скриптов
	mux.HandleFunc("GET /admin/state", s.adminState)
	mux.HandleFunc("GET /admin/config/{channel}", s.adminGetConfig)
	mux.HandleFunc("PUT /admin/config/{channel}", s.adminPutConfig)
	mux.HandleFunc("DELETE /admin/messages", s.adminReset)
	// Веб-интерфейс
	mux.HandleFunc("GET /{$}", s.uiPage)
	mux.HandleFunc("GET /ui/{channel}/live", s.uiLive)
	mux.HandleFunc("POST /ui/{channel}/config", s.uiConfig)
	mux.HandleFunc("POST /ui/{channel}/test", s.uiTest)
	mux.HandleFunc("DELETE /ui/messages", s.uiReset)
	mux.Handle("GET /static/", http.FileServerFS(assets))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8090"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("providersim запущен", "addr", srv.Addr, "config", base)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("сервер остановился с ошибкой", "err", err)
	}
	<-done
}
