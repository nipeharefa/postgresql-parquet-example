package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	w "github.com/xitongsys/parquet-go/writer"
)

// import
var cfg *viper.Viper

func init() {
	cfg = viper.New()

	cfg.AutomaticEnv()
	cfg.SetConfigType("yaml")
	replacer := strings.NewReplacer(".", "_")
	cfg.SetEnvKeyReplacer(replacer)

	cfg.SetConfigFile(`config.yaml`)
	err := cfg.ReadInConfig()
	if err != nil {
		log.Println(err)
	}
}

func connectDB(conn string) (*sqlx.DB, error) {

	db, err := sqlx.Open("postgres", conn)
	if err != nil {
		return nil, errors.New("error")

	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

type UserAccount struct {
	ID           string `parquet:"name=id, type=BYTE_ARRAY"`
	EventGroupID string `parquet:"name=event_group_id, type=BYTE_ARRAY"`
	ColorCode    string `parquet:"name=color_code, type=BYTE_ARRAY"`
	Date         int64  `parquet:"name=date, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	WeekName     string `parquet:"name=week_name, type=BYTE_ARRAY"`
}

type (
	PerikopenSong struct {
		ID           string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		PerikopenID  string `parquet:"name=perikopen_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		Type         string `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		SongNumber   string `parquet:"name=song_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		DisplayOrder int64  `parquet:"name=display_order, type=INT64"`
	}
)

func main() {

	db, err := connectDB(cfg.GetString("db.url"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	writeSong(db)
}

type (
	Column struct {
		Name     string `db:"column_name"`
		DataType string `db:"data_type"`
	}
)

func getParquetType(s string) string {

	switch s {
	case "uuid":
		return "UTF8"
	case "character varying":
		return "UTF8"
	}

	return ""
}
func generateSchema(db *sqlx.DB) []string {

	ret := []string{}
	var columns []Column
	err := db.Select(&columns, `select column_name, data_type from information_schema."columns" c where c.table_name = $1 order by ordinal_position`, "perikopen_song")
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(columns)

	for _, c := range columns {
		// fmt.Println(c.DataType)
		// ret = append(ret, fmt.Sprintf("name=%s, type=%s", c.Name, getParquetType(c.DataType)))
		parquetType := getParquetType(c.DataType)
		if parquetType == "UTF8" {
			ret = append(ret, fmt.Sprintf("name=%s, type=%s, encoding=PLAIN_DICTIONARY", c.Name, parquetType))
		}
	}

	// fmt.Println(ret)
	return ret
}
func writePerikopen(db *sqlx.DB) {
	pf, err := local.NewLocalFileWriter("nipe")
	if err != nil {
		log.Fatal(err)
	}

	pw, _ := w.NewParquetWriterFromWriter(pf, new(UserAccount), 4)
	pw.CompressionType = parquet.CompressionCodec_GZIP

	rows, err := db.Query("select id, event_group_id, date, color_code, week_name from perikopen p ;")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		u := UserAccount{}
		var d time.Time
		err := rows.Scan(&u.ID, &u.EventGroupID, &d, &u.ColorCode, &u.WeekName)
		if err != nil {
			log.Fatal(err)
		}
		u.Date = d.Unix()
		if err = pw.Write(u); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}

func writeSong(db *sqlx.DB) {

	pf, err := local.NewLocalFileWriter("perikopen.song.parquet")
	if err != nil {
		log.Fatal(err)
	}
	pw, _ := w.NewParquetWriterFromWriter(pf, new(PerikopenSong), 4)
	pw.CompressionType = parquet.CompressionCodec_GZIP

	rows, err := db.Query("select id, perikopen_id, type, song_number, display_order from perikopen_song")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		u := PerikopenSong{}
		// var d time.Time
		err := rows.Scan(&u.ID, &u.PerikopenID, &u.Type, &u.SongNumber, &u.DisplayOrder)
		if err != nil {
			log.Fatal(err)
		}
		// u.Date = d.Unix()
		if err = pw.Write(u); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}
