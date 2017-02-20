package influxdb

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client"
)

var (
	dbURL    = "http://10.10.141.51:8086"
	dbUser   = ""
	dbPasswd = ""
	dbName   = "liaoning"
)

func TestInfluxdb(t *testing.T) {
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.NewClient(client.Config{
		URL:      *u,
		Username: dbUser,
		Password: dbPasswd,
	})
	if err != nil {
		t.Fatalf("New Client failed: %v", err)
	}

	now := time.Now()
	pts := []client.Point{
		client.Point{
			Measurement: fmt.Sprintf("%s.count", "test"),
			Fields: map[string]interface{}{
				"value": 100,
			},
			Time: now},
	}
	b := client.BatchPoints{
		Points:   pts,
		Database: dbName,
	}
	_, err = c.Write(b)
	if err != nil {
		t.Logf("write failed: %v", err)
	}
}
