package influxkieker

import (
	"log"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestReadBatch(t *testing.T) {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("toml")
	viper.AddConfigPath("../.")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Print("Fatal error config file: %s \n", err)
	}

	viper.SetDefault("influxdb.addr", "http://localhost:8086")
	viper.SetDefault("influxdb.username", "root")
	viper.SetDefault("influxdb.password", "root")
	viper.SetDefault("influxdb.db.kieker", "kieker")
	viper.SetDefault("influxdb.db.k8s", "k8s")

	starttime, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Mar 21, 2017 at 4:00pm (CET)")
	endtime, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Mar 21, 2017 at 4:30pm (CET)")
	//starttime := time.Now()
	//endtime := time.Now().Add(60 * time.Second)
	log.Print("start=", starttime)
	log.Print("end", endtime)
	reader := &InfluxKiekerReader{
		Addr:      viper.GetString("influxdb.addr"),
		Username:  viper.GetString("influxdb.username"),
		Password:  viper.GetString("influxdb.password"),
		KiekerDb:  viper.GetString("influxdb.db.kieker"),
		K8sDb:     viper.GetString("influxdb.db.k8s"),
		Batch:     false,
		Starttime: starttime,
		Endtime:   endtime,
	}
	ch := reader.Read()
	for {
		_, ok := <-ch
		if ok {
			//log.Print(d)
		} else {
			break
		}
	}
}
