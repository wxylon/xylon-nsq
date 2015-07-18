package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/util"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"
)

type GraphTarget interface {
	Target(key string) (string, string)
}

type Topic struct {
	TopicName string
}

type Topics []*Topic

func TopicsFromStrings(s []string) Topics {
	t := make(Topics, 0, len(s))
	for _, ss := range s {
		tt := &Topic{ss}
		t = append(t, tt)
	}
	return t
}

func (t *Topic) Target(key string) (string, string) {
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("nsq.*.topic.%s.%s", t.TopicName, key)
	return target, color
}

type GraphInterval struct {
	Selected   bool
	Timeframe  string        // the UI string
	GraphFrom  string        // ?from=.
	GraphUntil string        // ?until=.
	Duration   time.Duration // for sort order
}

type GraphIntervals []*GraphInterval

func (g *GraphInterval) UrlOption() template.URL {
	return template.URL(fmt.Sprintf("t=%s", g.Timeframe))
}

func DefaultGraphTimeframes(selected string) GraphIntervals {
	var d GraphIntervals
	for _, t := range []string{"1h", "2h", "12h", "24h", "48h", "168h", "off"} {
		g, err := GraphIntervalForTimeframe(t, t == selected)
		if err != nil {
			log.Fatalf("error parsing duration %s", err.Error())
		}
		d = append(d, g)
	}
	return d
}

func GraphIntervalForTimeframe(t string, selected bool) (*GraphInterval, error) {
	if t == "off" {
		return &GraphInterval{
			Selected:   selected,
			Timeframe:  t,
			GraphFrom:  "",
			GraphUntil: "",
			Duration:   0,
		}, nil
	}
	duration, err := time.ParseDuration(t)
	if err != nil {
		return nil, err
	}
	start, end := startEndForTimeframe(duration)
	return &GraphInterval{
		Selected:   selected,
		Timeframe:  t,
		GraphFrom:  start,
		GraphUntil: end,
		Duration:   duration,
	}, nil
}

type GraphOptions struct {
	Configured        bool
	Enabled           bool
	GraphiteUrl       string
	UseStatsdPrefix   bool
	TimeframeString   template.URL
	AllGraphIntervals []*GraphInterval
	GraphInterval     *GraphInterval
}

func NewGraphOptions(rw http.ResponseWriter, req *http.Request, r *util.ReqParams) *GraphOptions {
	selectedTimeString, err := r.Get("t")
	if err != nil && selectedTimeString == "" {
		// get from cookie
		cookie, err := req.Cookie("t")
		if err != nil {
			selectedTimeString = "2h"
		} else {
			selectedTimeString = cookie.Value
		}
	} else {
		// set cookie
		host, _, _ := net.SplitHostPort(req.Host)
		cookie := &http.Cookie{
			Name:     "t",
			Value:    selectedTimeString,
			Path:     "/",
			Domain:   host,
			Expires:  time.Now().Add(time.Duration(720) * time.Hour),
			HttpOnly: true,
		}
		http.SetCookie(rw, cookie)
	}
	g, err := GraphIntervalForTimeframe(selectedTimeString, true)
	if err != nil {
		g, _ = GraphIntervalForTimeframe("2h", true)
	}
	base := *graphiteUrl
	configured := base != ""
	enabled := configured
	if *proxyGraphite {
		base = ""
	}
	if g.Timeframe == "off" {
		enabled = false
	}
	o := &GraphOptions{
		Configured:        configured,
		Enabled:           enabled,
		UseStatsdPrefix:   *useStatsdPrefixes,
		GraphiteUrl:       base,
		AllGraphIntervals: DefaultGraphTimeframes(selectedTimeString),
		GraphInterval:     g,
	}
	return o
}

func (g *GraphOptions) Prefix(metricType string) string {
	if g.UseStatsdPrefix && metricType == "counter" {
		return "stats_counts."
	} else if g.UseStatsdPrefix && metricType == "gauge" {
		return "stats.gauges."
	}
	return ""
}

func (g *GraphOptions) Sparkline(gr GraphTarget, key string) template.URL {
	target, color := gr.Target(key)
	target = g.Prefix(metricType(key)) + target
	params := url.Values{}
	params.Set("height", "20")
	params.Set("width", "120")
	params.Set("hideGrid", "true")
	params.Set("hideLegend", "true")
	params.Set("hideAxes", "true")
	params.Set("bgcolor", "ff000000") // transparent
	params.Set("fgcolor", "black")
	params.Set("margin", "0")
	params.Set("colorList", color)
	params.Set("yMin", "0")
	interval := fmt.Sprintf("%dsec", *statsdInterval/time.Second)
	params.Set("target", fmt.Sprintf(`summarize(sumSeries(%s),"%s","avg")`, target, interval))
	params.Set("from", g.GraphInterval.GraphFrom)
	params.Set("until", g.GraphInterval.GraphUntil)
	return template.URL(fmt.Sprintf("%s/render?%s", g.GraphiteUrl, params.Encode()))
}

func (g *GraphOptions) LargeGraph(gr GraphTarget, key string) template.URL {
	target, color := gr.Target(key)
	target = g.Prefix(metricType(key)) + target
	params := url.Values{}
	params.Set("height", "450")
	params.Set("width", "800")
	params.Set("bgcolor", "ff000000") // transparent
	params.Set("fgcolor", "999999")
	params.Set("colorList", color)
	params.Set("yMin", "0")
	interval := fmt.Sprintf("%dsec", *statsdInterval/time.Second)
	target = fmt.Sprintf(`summarize(sumSeries(%s),"%s","avg")`, target, interval)
	if metricType(key) != "gauge" {
		scale := fmt.Sprintf("%.04f", 1/float64(*statsdInterval/time.Second))
		target = fmt.Sprintf(`scale(%s,%s)`, target, scale)
	}
	params.Set("target", target)
	params.Set("from", g.GraphInterval.GraphFrom)
	params.Set("until", g.GraphInterval.GraphUntil)
	return template.URL(fmt.Sprintf("%s/render?%s", g.GraphiteUrl, params.Encode()))
}

func (g *GraphOptions) Rate(gr GraphTarget) string {
	target, _ := gr.Target("message_count")
	return g.Prefix(metricType("message_count")) + target
}

func metricType(key string) string {
	metricType := "counter"
	switch key {
	case "backend_depth", "depth", "clients", "in_flight_count", "deferred_count":
		metricType = "gauge"
	}
	return metricType
}

func rateQuery(target string) string {
	params := url.Values{}
	fromInterval := fmt.Sprintf("-%dsec", *statsdInterval*2/time.Second)
	params.Set("from", fromInterval)
	untilInterval := fmt.Sprintf("-%dsec", *statsdInterval/time.Second)
	params.Set("until", untilInterval)
	params.Set("format", "json")
	params.Set("target", fmt.Sprintf("sumSeries(%s)", target))
	return fmt.Sprintf("/render?%s", params.Encode())
}

func parseRateResponse(body []byte) ([]byte, error) {
	js, err := simplejson.NewJson([]byte(body))
	if err != nil {
		log.Printf("ERROR: failed to parse metadata - %s", err.Error())
		return nil, err
	}

	js, ok := js.GetIndex(0).CheckGet("datapoints")
	if !ok {
		return nil, errors.New("datapoints not found")
	}

	var rateStr string
	rate, _ := js.GetIndex(0).GetIndex(0).Float64()
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := *statsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return json.Marshal(map[string]string{"datapoint": rateStr})
}

func startEndForTimeframe(t time.Duration) (string, string) {
	start := fmt.Sprintf("-%dmin", int(t.Minutes()))
	return start, "-1min"
}
