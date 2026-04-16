package source

import (
	"context"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)
type Source interface {
	Name() string
	Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint) (msgChan <-chan []protocol.Message, ackChan chan<- struct{}, err error)
	Stop() error
	AlterPublication(ctx context.Context, tableName string) error
	RestartWithNewTables(ctx context.Context, newTables []string) error
}
