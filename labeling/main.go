package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Influxdb struct {
		Url   string `koanf:"url"`
		Org   string `koanf:"org"`
		Token string `koanf:"token"`
	} `koanf:"influxdb"`
	// Pg struct {
	// 	connection string `koanf:"connection"`
	// } `koanf:"pg"`

	StartTime string `koanf:"start"`
	EndTime   string `koanf:"end"`

	Bucket string `koanf:"bucket"`

	Measurement string  `koanf:"measurement"`
	Value       float64 `koanf:"value"`
}

var k = koanf.New(".")

var db *sql.DB

// func connectPg() {
// 	var err error
// 	db, err = sql.Open("postgres", k.MustString("pg.connection"))
// 	if err != nil {
// 		log.Fatal().Err(err)
// 	}

// }

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	pf := pflag.NewFlagSet("config", pflag.ExitOnError)
	pf.Usage = func() {
		fmt.Println(pf.FlagUsages())
		os.Exit(0)
	}

	pf.String("config", "config.yaml", "config file")
	pf.String("bucket", "test", "bucket")
	pf.String("measurement", "test", "new measurement name")
	pf.Float64("value", 0, "value")
	pf.String("start", "2006-01-02 15:04:05", "start time")
	pf.String("end", "2006-01-02 15:04:05", "end time")
	pf.Parse(os.Args[1:])

	if err := k.Load(file.Provider(pf.Lookup("config").Value.String()), yaml.Parser()); err != nil {
		log.Fatal().Err(err)
	}

	if err := k.Load(posflag.Provider(pf, ".", k), nil); err != nil {
		log.Fatal().Msgf("error loading config: %v", err)
	}

	// Unmarshal the loaded config into a struct.
	var c Config
	if err := k.Unmarshal("", &c); err != nil {
		log.Fatal().Err(err)
	}

	log.Debug().Msgf("config: %+v", c)

	token := k.String("influxdb.token")
	url := k.String("influxdb.url")
	org := k.String("influxdb.org")

	client := influxdb2.NewClient(url, token)
	defer client.Close()
	queryAPI := client.QueryAPI(org)
	// Get points from Vehicle speed

	start, err := time.ParseInLocation("2006-01-02 15:04:05", k.String("start"), time.Local)
	if err != nil {
		log.Fatal().Err(err)
	}

	end, err := time.ParseInLocation("2006-01-02 15:04:05", k.String("end"), time.Local)
	if err != nil {
		log.Fatal().Err(err)
	}

	bucket := k.String("bucket")
	query := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %d, stop: %d)
  |> filter(fn: (r) => r["_measurement"] == "Vehicle speed")
  |> filter(fn: (r) => r["_field"] == "value")
	`,
		bucket,
		start.Unix(),
		end.Unix(),
	)

	log.Debug().Msg(query)

	res, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Fatal().Err(err)
	}

	if res == nil {
		log.Fatal().Msg("No data")
	}

	value := k.Float64("value")

	deleteAPI := client.DeleteAPI()
	orgObj, err := client.OrganizationsAPI().FindOrganizationByName(context.Background(), org)
	if err != nil {
		log.Fatal().Err(err)
	}
	bucketObj, err := client.BucketsAPI().FindBucketByName(context.Background(), bucket)
	if err != nil {
		log.Fatal().Err(err)
	}
	if bucketObj == nil {
		log.Info().Msgf("Creating bucket %s in org %s", bucket, orgObj.Name)

		bucketObj, err = client.BucketsAPI().CreateBucketWithName(context.Background(), orgObj, bucket)
		if err != nil {
			log.Fatal().Err(err)
		}
	}

	log.Info().Msgf("Deleting data from %s to %s for bucket %s in org %s", start, end, bucketObj.Name, orgObj.Name)
	deleteAPI.Delete(context.Background(), orgObj, bucketObj, start, end, fmt.Sprintf("_measurement=\"%s\"", k.String("measurement")))

	newMeasuremnt := []*write.Point{}
	for res.Next() {
		pointTimestamp := res.Record().Time()
		if pointTimestamp.After(end) || pointTimestamp.Before(start) {
			continue
		} else {
			// log.Debug().Msgf("pointTimestamp: %v", pointTimestamp)
			newPoint := influxdb2.NewPoint(k.String("measurement"), map[string]string{}, map[string]interface{}{
				"value": value,
			}, pointTimestamp)
			newMeasuremnt = append(newMeasuremnt, newPoint)
		}
	}

	writeAPI := client.WriteAPI(org, k.String("bucket"))
	for _, p := range newMeasuremnt {
		writeAPI.WritePoint(p)
	}
}
