package main

import (
	"log"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type UserAccount struct {
	ID           string `parquet:"name=id, type=BYTE_ARRAY"`
	EventGroupID string `parquet:"name=event_group_id, type=BYTE_ARRAY"`
	ColorCode    string `parquet:"name=color_code, type=BYTE_ARRAY"`
	Date         int64  `parquet:"name=date, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	WeekName     string `parquet:"name=week_name, type=BYTE_ARRAY"`
}

func main() {

	f, err := os.Open("../nipe")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	fr, err := local.NewLocalFileReader("../nipe")
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, new(UserAccount), 4)
	if err != nil {
		log.Fatal(err)
	}
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]UserAccount, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
	}
	pr.ReadStop()
	fr.Close()
}
