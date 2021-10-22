package util

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/cobrautil"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// ZeroLogPreRunEFunc returns a  cobra PreRunE function that wires zerolog into
// the given IO streams
func ZeroLogPreRunEFunc(out io.Writer) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if cobrautil.IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		tty := false
		if f, ok := out.(*os.File); ok {
			tty = isatty.IsTerminal(f.Fd())
		}
		format := cobrautil.MustGetString(cmd, "log-format")
		if format == "human" || (format == "auto" && tty) {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: out})
		} else {
			log.Logger = log.Output(out)
		}

		levelString := strings.ToLower(cobrautil.MustGetString(cmd, "log-level"))
		level, err := zerolog.ParseLevel(levelString)
		if err != nil {
			return fmt.Errorf("unknown log level: %s", levelString)
		}
		zerolog.SetGlobalLevel(level)
		log.Info().Str("new level", levelString).Msg("set log level")
		return nil
	}
}

// LoggedConnConfig wraps a pgx.ConnConfig to make it satisfy the
// zerolog.LogObjectMarshaler interface
type LoggedConnConfig struct {
	*pgx.ConnConfig
}

// MarshalZerologObject satisfies the zerolog.LogObjectMarshaler interface
func (l LoggedConnConfig) MarshalZerologObject(e *zerolog.Event) {
	e.Str("host", l.Host)
	e.Str("user", l.User)
	e.Str("database", l.Database)
}
