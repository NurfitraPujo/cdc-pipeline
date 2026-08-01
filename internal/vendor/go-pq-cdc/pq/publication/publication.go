package publication

import (
	"context"
	goerrors "errors"
	"strings"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorPublicationIsNotExists = goerrors.New("publication is not exists")
)

var typeMap = pgtype.NewMap()

type Publication struct {
	conn pq.Connection
	cfg  Config
}

func New(cfg Config, conn pq.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (c *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorPublicationIsNotExists) || !c.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "publication info")
		}
	} else {
		logger.Warn("publication already exists")
		return info, nil
	}

	resultReader := c.conn.Exec(ctx, c.cfg.createQuery())
	_, err = resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication create result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication create result reader close")
	}

	logger.Info("publication created", "name", c.cfg.Name)

	return &c.cfg, nil
}

func (c *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := c.conn.Exec(ctx, c.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "42703" {
			return nil, ErrorPublicationIsNotExists
		}
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorPublicationIsNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication info result reader close")
	}

	publicationInfo, err := decodePublicationInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "publication info result decode")
	}

	return publicationInfo, nil
}

func decodePublicationInfoResult(result *pgconn.Result) (*Config, error) {
	var publicationConfig Config
	var tables []string

	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "pubname":
			publicationConfig.Name = v.(string)
		case "pubinsert":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "INSERT")
			}
		case "pubupdate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "UPDATE")
			}
		case "pubdelete":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "DELETE")
			}
		case "pubtruncate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "TRUNCATE")
			}
		case "pubtables":
			for _, val := range v.([]any) {
				tables = append(tables, val.(string))
			}
		}
	}

	// vendored-patch: MS-2 (MULTI_SCHEMA_PLAN.md §3 Stage 4, task 2) - tableName
	// is produced by infoQuery's `schemaname || '.' || tablename` concat
	// (config.go), which guarantees exactly one schema/table separator but says
	// nothing about additional dots inside tablename itself (quoted Postgres
	// identifiers may legally contain "."). The previous `strings.Split` +
	// `st[1]`/`st[0]` indexing (a) panicked on an unqualified name (len(st) < 2 --
	// unreachable today given the concat, but not guaranteed by the type system)
	// and (b) silently misparsed a table name containing a literal "." by
	// assigning only the first fragment to Name and dropping the rest.
	// SplitN(..., 2) fixes (b) by keeping everything after the first "." as the
	// table name, and an explicit length/emptiness guard fixes (a) by skipping
	// the entry (with a warning) instead of panicking.
	for _, tableName := range tables {
		st := strings.SplitN(tableName, ".", 2)
		if len(st) != 2 || st[0] == "" || st[1] == "" {
			logger.Warn("publication info: skipping unparseable qualified table name", "table", tableName)
			continue
		}
		publicationConfig.Tables = append(publicationConfig.Tables, Table{
			Name:   st[1],
			Schema: st[0],
		})
	}

	return &publicationConfig, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
