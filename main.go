package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var globalSqlDB *sql.DB
var sqlDsn = fmt.Sprintf("%s:%s@tcp(%s)/fu_dev?parseTime=true&charset=utf8",
	os.Getenv("dbms_user"), os.Getenv("dbms_password"), os.Getenv("dbms"))
var sqlRemote = fmt.Sprintf("%s:3306", os.Getenv("dbms"))
var portToListen = 3306

const pingDuration = time.Minute * 1

func main() {
	ctx := context.Background()

	log.Printf("Opening a DB connection %s\n", sqlDsn)
	globalSqlDB = initDb(sqlDsn)

	log.Printf("Starting listening on %d\n", portToListen)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", portToListen))
	if err != nil {
		log.Panicln(err)
	}
	go func() {
		if err := sqlPinger(ctx); err != nil {
			log.Panicln(err)
		}
	}()

	defer func() {
		log.Println("Closing connections ...")

		if e := l.Close(); e != nil {
			log.Println(e)
		}

		if e := globalSqlDB.Close(); e != nil {
			log.Println(e)
		}
	}()

	clients := make(map[string]net.Conn)
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
		}

		remoteAddr := c.RemoteAddr().String()
		log.Printf("forwarding %s to SQL\n", remoteAddr)

		clients[remoteAddr] = c
		// forwarding
		go func(c net.Conn, remoteAddr string) {
			defer func() {
				log.Printf("Close client connection: %s\n", c.RemoteAddr().String())
				if err := c.Close(); err != nil {
					log.Println(err)
				}
			}()

			log.Printf("handling a connection: %s\n", remoteAddr)
			log.Printf("Dialing a remote SQL server %s\n", sqlRemote)
			sqlResource, err := net.Dial("tcp", sqlRemote)
			if err != nil {
				log.Printf("err: %s\n", err.Error())
				return
			}

			defer func() {
				log.Println("Close SQL connection")
				if err := sqlResource.Close(); err != nil {
					log.Println(err)
				}
			}()

			if err := handleConnection(sqlResource, c); err != nil {
				log.Printf("failed to handle a connection: %s\n", err.Error())
			} else {
				log.Printf("done with a connection: %s\n", remoteAddr)
			}
		}(c, remoteAddr)
	}
}

func handleConnection(sqlResource, clientResource net.Conn) error {
	errorChan := make(chan error)
	clientDataChan := make(chan []byte)
	serverDataChan := make(chan []byte)
	go readWrapper(clientResource, clientDataChan, errorChan)
	go readWrapper(sqlResource, serverDataChan, errorChan)

	for {
		select {
		case data := <-clientDataChan:
			if n, err := sqlResource.Write(data); err != nil {
				log.Printf("failed to write to sql: %s\n", err.Error())
				return err
			} else {
				log.Printf("writing back %d bytes to a SQL\n", n)
			}
		case data := <-serverDataChan:
			if n, err := clientResource.Write(data); err != nil {
				log.Printf("failed to write to a client: %s\n", err.Error())
				return err
			} else {
				log.Printf("writing back %d bytes to a client\n", n)
			}
		case err := <-errorChan:
			if err == io.EOF {
				return nil
			}
			log.Println("An error occurred:", err.Error())
			return err
		}
	}
}

func readWrapper(conn net.Conn, dataChan chan []byte, errorChan chan error) {
	for {
		buf := make([]byte, 1024)
		reqLen, err := conn.Read(buf)
		if err != nil {
			errorChan <- err
			return
		}
		dataChan <- buf[:reqLen]
	}
}

func sqlPinger(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pingDuration):
			if globalSqlDB != nil {
				if isConnected(globalSqlDB) {
					log.Println("WARNING: SQL host not available!")
				}
			} else {
				return errors.New("unable to ping on non-connected db")
			}
		}
	}
}

func initDb(dsn string) *sql.DB {
	log.Printf("Opening a connection to \"%s\"\n", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	if isConnected(db) {
		return db
	}

	log.Println("Failed. Waiting...")

	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan struct{})

	for {
		select {
		case <-ticker.C:
			log.Println("Checking the connection ... ")
			if isConnected(db) {
				close(quit)
			}
			log.Println("Failed. Waiting...")
		case <-quit:
			log.Println("Connected")
			ticker.Stop()
			return db
		}
	}
}

func isConnected(db *sql.DB) bool {
	log.Println("PING")
	err := db.Ping()
	return err == nil
}
