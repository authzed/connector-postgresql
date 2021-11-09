package options

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	"sigs.k8s.io/yaml"

	"github.com/authzed/connector-postgresql/pkg/config"
	"github.com/authzed/connector-postgresql/pkg/pgschema"
	"github.com/authzed/connector-postgresql/pkg/streams"
	"github.com/authzed/connector-postgresql/pkg/util"
)

type ConfigPrinter func(c *config.Config) error

func DiscardConfigPrinter(*config.Config) error {
	return nil
}

var _ ConfigPrinter = DiscardConfigPrinter

func JSONConfigPrinter(w io.Writer) ConfigPrinter {
	return func(c *config.Config) error {
		configJSON, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, string(configJSON)); err != nil {
			return err
		}
		return nil
	}
}

func YAMLConfigPrinter(w io.Writer) ConfigPrinter {
	return func(c *config.Config) error {
		configYaml, err := yaml.Marshal(c)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, string(configYaml)); err != nil {
			return err
		}
		return nil
	}
}

type ConfigOptions struct {
	MappingFile string

	Config *config.Config

	ConfigPrinter ConfigPrinter
}

func (o *ConfigOptions) Complete(ctx context.Context, replogConfig *pgxpool.Config, streams streams.IO) error {
	if o.Config != nil {
		log.Debug().Msg("mapping config already set, skipping mapping option validation")
		o.ConfigPrinter = DiscardConfigPrinter
		return nil
	}
	if len(o.MappingFile) > 0 {
		log.Info().Str("config", o.MappingFile).Msg("loading mapping config from file")
		if o.Config == nil {
			mapFileContents, err := os.ReadFile(o.MappingFile)
			if err != nil {
				return err
			}
			if err := yaml.Unmarshal(mapFileContents, &o.Config); err != nil {
				return err
			}
		}
		o.ConfigPrinter = DiscardConfigPrinter
		return nil
	}
	log.Info().Msg("generating zed schema and mapping config from postgres")
	log.Info().EmbedObject(util.LoggedConnConfig{ConnConfig: replogConfig.ConnConfig}).Msg("connecting to postgres")

	replogConn, err := pgxpool.ConnectConfig(ctx, replogConfig)
	if err != nil {
		return err
	}
	defer replogConn.Close()

	log.Info().Msg("syncing postgres schema")
	schema, err := pgschema.SyncSchema(ctx, replogConn)
	if err != nil {
		return err
	}
	o.Config = schema.ToConfig()
	o.ConfigPrinter = YAMLConfigPrinter(streams.Out)
	return nil
}
