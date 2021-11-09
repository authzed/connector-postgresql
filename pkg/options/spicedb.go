package options

import (
	"fmt"

	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

type SpiceDBOptions struct {
	SpiceDBEndpoint string
	SpiceDBToken    string
	SpiceDBInsecure bool

	Client *authzed.Client
}

func (o *SpiceDBOptions) Complete(dryRun bool) (err error) {
	if dryRun {
		return nil
	}
	if o.Client != nil {
		log.Debug().Msg("spicedb client already configured, skipping client option validation")
		return
	}
	if o.SpiceDBEndpoint == "" {
		return fmt.Errorf("must provide spicedb uri")
	}
	grpcOpts := make([]grpc.DialOption, 0)
	if o.SpiceDBInsecure == true {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}
	if o.SpiceDBToken != "" && o.SpiceDBInsecure {
		grpcOpts = append(grpcOpts, grpcutil.WithInsecureBearerToken(o.SpiceDBToken))
	}
	if o.SpiceDBToken != "" && !o.SpiceDBInsecure {
		grpcOpts = append(grpcOpts, grpcutil.WithBearerToken(o.SpiceDBToken))
	}
	o.Client, err = authzed.NewClient(o.SpiceDBEndpoint, grpcOpts...)
	return
}
