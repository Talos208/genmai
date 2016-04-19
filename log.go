package genmai

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"
)

const defaultLoggingFormat = `[{{.time.Format "2006-01-02 15:04:05"}}] [{{.duration}}] {{.query}}`

var (
	defaultLoggerTemplate = template.Must(template.New("genmai").Parse(defaultLoggingFormat))
	defaultLogger         = &nullLogger{}

	splitWhereRegex  = regexp.MustCompile("WHERE")
	updateRegex      = regexp.MustCompile(`UPDATE.*SET\s*(.*)`)
	updateParamRegex = regexp.MustCompile("`(\\w+?)`\\s*=\\s*\\?")
	insertRegex      = regexp.MustCompile(`INSERT.*\((.+?)\)\s*VALUES`)
	insertParamRegex = regexp.MustCompile("`(\\w+?)`")
	whereRegex       = regexp.MustCompile("`(\\w+?)`\\s*(?:=\\s*(\\?)|IN\\s*\\(([\\?\\s,]+)\\))")
)

// logger is the interface that query logger.
type logger interface {
	// Print outputs query log.
	Print(start time.Time, query string, args ...interface{}) error

	// SetFormat sets the format for logging.
	SetFormat(format string) error

	// SetSlowTime sets slow query threshold.
	SetSlowTime(slow float64) error

	// AddColumnMask sets column to be masked.
	AddColumnMask(mask string)

	// AddColumnMask remove column to be masked.
	RemoveColumnMask(mask string)
}

// templateLogger is a logger that Go's template to be used as a format.
// It implements the logger interface.
type templateLogger struct {
	w  io.Writer
	t  *template.Template
	m  sync.Mutex
	s  float64
	mc []string
}

// SetFormat sets the format for logging.
func (l *templateLogger) SetFormat(format string) error {
	l.m.Lock()
	defer l.m.Unlock()
	t, err := template.New("genmai").Parse(format)
	if err != nil {
		return err
	}
	l.t = t
	return nil
}

func (l *templateLogger) SetSlowTime(slow float64) error {
	l.s = slow
	return nil
}

func (l *templateLogger) AddColumnMask(mask string) {
	l.mc = append(l.mc, mask)
}

func (l *templateLogger) RemoveColumnMask(mask string) {
	for i, v := range l.mc {
		if v == mask {
			l.mc = append(l.mc[:i], l.mc[i+1:]...)
			return
		}
	}
}

func (l *templateLogger) toMaskColumn(sql string) []int {
	// Gather target columns from
	s2 := splitWhereRegex.Split(string(sql), -1)

	var cols []string
	su := updateRegex.FindSubmatch([]byte(s2[0]))
	if len(su) > 1 {
		for _, v := range updateParamRegex.FindAllSubmatch(su[1], -1) {
			cols = append(cols, string(v[1]))
		}
	} else if si := insertRegex.FindSubmatch([]byte(s2[0])); len(si) > 1 {
		for _, v := range insertParamRegex.FindAllSubmatch(si[1], -1) {
			cols = append(cols, string(v[1]))
		}
	}
	if len(s2) > 1 {
		sw := whereRegex.FindAllSubmatch([]byte(s2[1]), -1)
		for _, v := range sw {
			cnt := 0
			for _, w := range v[2:] {
				cnt += bytes.Count(w, []byte{'?'})
			}
			for i := 0; i < cnt; i++ {
				cols = append(cols, string(v[1]))
			}
		}
	}

	to_mask := []int{}
	for i, c := range cols {
		for _, m := range l.mc {
			if c == m {
				to_mask = append(to_mask, i)
				break
			}
		}
	}

	return to_mask
}

// Print outputs query log using format template.
// All arguments will be used to formatting.
func (l *templateLogger) Print(start time.Time, query string, args ...interface{}) error {
	if len(args) > 0 {
		// Mask
		to_mask := l.toMaskColumn(query)
		values := make([]string, len(args))
		for i, arg := range args {
			if len(to_mask) > 0 && to_mask[0] == i {
				values[i] = "* SECRET *"
				to_mask = to_mask[1:]
			} else {
				values[i] = fmt.Sprintf("%#v", arg)
			}
		}
		query = fmt.Sprintf("%v; [%v]", query, strings.Join(values, ", "))
	} else {
		query = fmt.Sprintf("%s;", query)
	}
	duration := now().Sub(start).Seconds() * 1000.0
	if l.s <= 0.0 || duration >= l.s {
		data := map[string]interface{}{
			"time":     start,
			"duration": fmt.Sprintf("%.2fms", duration),
			"query":    query,
		}
		var buf bytes.Buffer
		if err := l.t.Execute(&buf, data); err != nil {
			return err
		}
		l.m.Lock()
		defer l.m.Unlock()
		if _, err := fmt.Fprintln(l.w, strings.TrimSuffix(buf.String(), "\n")); err != nil {
			return err
		}
	}
	return nil
}

// nullLogger is a null logger.
// It implements the logger interface.
type nullLogger struct{}

// SetFormat is a dummy method.
func (l *nullLogger) SetFormat(format string) error {
	return nil
}

func (l *nullLogger) SetSlowTime(slow float64) error {
	return nil
}

func (l *nullLogger) AddColumnMask(mask string) {
}

func (l *nullLogger) RemoveColumnMask(mask string) {
}

// Print is a dummy method.
func (l *nullLogger) Print(start time.Time, query string, args ...interface{}) error {
	return nil
}
