package options

import (
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
)

// PostgresOptions holds options related to postgres
type PostgresOptions struct {
	PostgresURI string

	ReplogConfig *pgxpool.Config
	PoolConfig   *pgxpool.Config
}

// Complete configures postgres options from a URI if needed
// Set either URI or the config objects, but not both.
func (o *PostgresOptions) Complete() error {
	if o.PoolConfig != nil && o.ReplogConfig != nil {
		log.Debug().Msg("postrgres config already set, skipping postgres option validation")
		return nil
	}
	if o.PoolConfig == nil && o.ReplogConfig == nil {
		if o.PostgresURI == "" {
			return fmt.Errorf("must provide postgres uri or dsn")
		}

		cfg, err := pgxpool.ParseConfig(o.PostgresURI)
		if err != nil {
			return err
		}
		o.PoolConfig = cfg

		// configure a (limited) connection for watching the replication log
		repcfg, err := pgxpool.ParseConfig(o.PostgresURI + "&replication=database")
		if err != nil {
			return err
		}
		// replication connections don't support extended query protocol
		repcfg.ConnConfig.PreferSimpleProtocol = true
		o.ReplogConfig = repcfg
		return nil
	}

	// only reachable from incorrect library usage
	log.Fatal().Str("pg uri", o.PostgresURI).Msg("import options incomplete: either set postgres uri, or manually configure the connections")

	return nil
}
