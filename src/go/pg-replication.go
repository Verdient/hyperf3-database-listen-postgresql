package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/erikdubbelboer/gspt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func isProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}

	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}

	if errors.Is(err, syscall.ESRCH) {
		return false
	}

	return true
}

func setProcessNameFromEnv() {
	name := os.Getenv("PG_PROCESS_NAME")
	if name == "" {
		return
	}
	gspt.SetProcTitle(name)
}

func checkMasterProcess() {
	var masterPID int

	if pidStr := os.Getenv("PG_MASTER_PID"); pidStr != "" {
		if val, err := strconv.Atoi(pidStr); err == nil {
			masterPID = val
			go func() {
				for {
					time.Sleep(1 * time.Second)
					if !isProcessRunning(masterPID) {
						log.Printf("Parent process %d exited. Exiting...", masterPID)
						p, _ := os.FindProcess(os.Getpid())
						_ = p.Signal(os.Interrupt)
						return
					}
				}
			}()
		}
	}
}

func main() {
	setProcessNameFromEnv()

	checkMasterProcess()

	dsn := os.Getenv("PG_DSN")
	slot := os.Getenv("PG_SLOT")
	if dsn == "" || slot == "" {
		log.Fatal("PG_DSN and PG_SLOT must be set")
	}

	ctx := context.Background()

	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("ParseConfig failed: %v", err)
	}

	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close(ctx)

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Fatalf("IdentifySystem failed: %v", err)
	}

	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		slot,
		"wal2json",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)

	pluginArguments := []string{
		"\"include-xids\" '1'",
		"\"write-in-chunks\" '1'",
		"\"format-version\" '2'",
	}

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		slot,
		sys.XLogPos,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pluginArguments,
		},
	)
	if err != nil {
		log.Fatalf("StartReplication failed: %v", err)
	}

	var lastLSN = sys.XLogPos
	standbyTimeout := 10 * time.Second
	nextStatus := time.Now().Add(standbyTimeout)

	for {
		if time.Now().After(nextStatus) {
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: lastLSN,
					WALFlushPosition: lastLSN,
					WALApplyPosition: lastLSN,
					ClientTime:       time.Now(),
				},
			)
			if err != nil {
				log.Printf("status update failed: %v", err)
			}
			nextStatus = time.Now().Add(standbyTimeout)
		}

		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			log.Fatalf("ReceiveMessage failed: %v", err)
		}

		switch m := msg.(type) {

		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}

			switch m.Data[0] {

			case 'w':
				xlog, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					log.Printf("ParseXLogData failed: %v", err)
					continue
				}

				lastLSN = xlog.WALStart + pglogrepl.LSN(len(xlog.WALData))
				fmt.Println(string(xlog.WALData))

			case 'k':
				keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					log.Printf("ParseKeepalive failed: %v", err)
					continue
				}

				if keepalive.ReplyRequested {
					_ = pglogrepl.SendStandbyStatusUpdate(
						ctx,
						conn,
						pglogrepl.StandbyStatusUpdate{
							WALWritePosition: lastLSN,
							ClientTime:       time.Now(),
						},
					)
				}
			}

		default:
		}
	}
}
