package follow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/rs/zerolog/log"

	"github.com/authzed/connector-postgres/pkg/cache"
	"github.com/authzed/connector-postgres/pkg/config"
)

const pgOutputPlugin = "pgoutput"

// Follower is the interface for things that can follow a WAL
type Follower interface {
	Follow(ctx context.Context, startingpos pglogrepl.LSN) error
}

// WalFollower watches the WAL and writes changes into the cache
type WalFollower struct {
	conn    *pgconn.PgConn
	mapping config.InternalTableMapping
	cache   *cache.Cache
}

// NewWalFollower creates a new WalFollower for postgres. The conn must be made
// with the `replication` flag set.
func NewWalFollower(conn *pgconn.PgConn, mapping config.InternalTableMapping, cache *cache.Cache) *WalFollower {
	return &WalFollower{
		conn:    conn,
		mapping: mapping,
		cache:   cache,
	}
}

// Follow starts watching the replication log at startpos. It uses the config's
// InternalTableMapping to translate WAL events into relationships, which are
// then written to the cache.
// Follow (and replication connections in general) are not safe to share across
// threads. Events should be read from the cache to process them in parallel.
func (f *WalFollower) Follow(ctx context.Context, startpos pglogrepl.LSN) error {
	log.Warn().Msg("Replication does not properly support deleting relationships, do not use for production.")
	publication := "spicedb_sync"

	// TODO: should publication be an arg instead?
	result := f.conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION %s;", publication))
	_, _ = result.ReadAll()

	result = f.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", publication))
	_, err := result.ReadAll()
	if err != nil {
		return err
	}

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", publication)}

	slotName := newSlotName("spicedb_sync_slot")
	_, err = pglogrepl.CreateReplicationSlot(ctx, f.conn, slotName, pgOutputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		return err
	}
	err = pglogrepl.StartReplication(ctx, f.conn, slotName, startpos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return err
	}
	defer func() {
		log.Info().Str("slot", slotName).Msg("dropping replication slot")
		if err := pglogrepl.DropReplicationSlot(ctx, f.conn, slotName, pglogrepl.DropReplicationSlotOptions{Wait: false}); err != nil {
			log.Warn().Err(err).Str("slot", slotName).Msg("failed to drop replication slot")
		} else {
			log.Info().Str("slot", slotName).Msg("replication slot dropped")
		}
	}()

	clientXLogPos := startpos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, f.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return err
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		msg, err := f.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				log.Trace().Stringer("WALStart", xld.WALStart).Stringer("ServerWALEnd", xld.ServerWALEnd).Time("ServerTime", xld.ServerTime).Str("WALData", string(xld.WALData)).Msg("received XLogData")
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return err
				}
				switch logicalMsg.Type() {
				case pglogrepl.MessageTypeInsert:
					insertMsg := logicalMsg.(*pglogrepl.InsertMessage)
					rels := f.pgTupleToRelationships(insertMsg.RelationID, insertMsg.Tuple)
					for _, rel := range rels {
						f.cache.Touch(rel)
					}
				case pglogrepl.MessageTypeDelete:
					// TODO: WARNING: DELETEs need to be handled differently
					// pgtuples may not contain enough data to convert to spicedbtuples
					// instead, we need to translate them into deleterelationship requests
					// that match the filters implied by the row
					log.Warn().Stringer("type", logicalMsg.Type()).Msg("DELETE is not fully supported by the connector")
					deleteMsg := logicalMsg.(*pglogrepl.DeleteMessage)
					rels := f.pgTupleToRelationships(deleteMsg.RelationID, deleteMsg.OldTuple)
					for _, rel := range rels {
						f.cache.Delete(rel)
					}
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Warn().Str("msg", fmt.Sprintf("%#v", msg)).Msg("received unexpected message")
		}
	}
}

func (f *WalFollower) pgTupleToRelationships(relationID uint32, data *pglogrepl.TupleData) []*v1.Relationship {
	cols := data.Columns
	tlog := log.Trace().Uint32("relationID", relationID)
	for i, c := range cols {
		tlog = tlog.Bytes(fmt.Sprintf("col-%d", i+1), c.Data)
	}
	tlog.Msg("received tuple")

	rels := make([]*v1.Relationship, 0)
	for _, rm := range f.mapping[relationID] {
		rescols := make([]string, 0, len(rm.ResourceIDCols))
		for _, i := range rm.ResourceIDCols {
			// column numbers are 1-indexed
			rescols = append(rescols, string(cols[i-1].Data))
		}
		subcols := make([]string, 0, len(rm.SubjectIDCols))
		for _, i := range rm.SubjectIDCols {
			// column numbers are 1-indexed
			subcols = append(subcols, string(cols[i-1].Data))
		}

		rel := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: rm.ResourceType,
				ObjectId:   strings.Join(rescols, "_"),
			},
			Relation: rm.Relation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: rm.SubjectType,
					ObjectId:   strings.Join(subcols, "_"),
				},
			},
		}
		rels = append(rels, rel)
	}
	return rels
}

// newSlotName can panic and should only be called during process init
func newSlotName(prefix string) string {
	token := make([]byte, 5)
	if _, err := rand.Read(token); err != nil {
		panic("couldn't get random bytes")
	}
	return strings.Join([]string{prefix, hex.EncodeToString(token)}, "_")
}
