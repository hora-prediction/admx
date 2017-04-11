package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hora-prediction/admx/influxkieker"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var htmlpage = []byte(`
<html><body>
		<form action="/admx" method="post">
			<br />
			Address of Hora: <input type="text" text name="hora.addr" value="http://hora:8080/adm" />
			<br />
			Duration: <input type="text" text name="duration" value="1"/> Minutes
			<br />
			<input type="submit" value="Update"/>
		</form>
	</body></html>
`)

func main() {
	// Read configurations
	log.Println("Reading configuration")
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Println("Fatal error config file: %s \n", err)
	}

	viper.SetEnvPrefix("hora") // will be uppercased automatically
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// TODO: use new structure

	batchMode := viper.GetBool("admx.batch")
	if batchMode {
		starttime := viper.GetTime("admx.starttime")
		endtime := viper.GetTime("admx.endtime")
		startBatchExtraction(starttime, endtime)
	} else {
		Serve()
	}
}

func Serve() {
	log.Print("Starting ADMX Web UI")
	port := viper.GetString("admx.webui.port")
	r := mux.NewRouter()
	r.HandleFunc("/admx", handler).Methods("GET")
	r.HandleFunc("/admx", posthandler).Methods("POST")
	srv := &http.Server{
		Handler: r,
		Addr:    ":" + port,
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Write(htmlpage)
}

func posthandler(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	duration := req.Form.Get("duration")
	durationTime, err := time.ParseDuration(duration + "m")
	horaAddr := req.Form.Get("hora.addr")
	if err != nil {
		w.Write([]byte("Error parsing " + duration))
	}
	go startRealtimeExtraction(durationTime, horaAddr)
	w.Write([]byte(fmt.Sprintf("Starting ADM extraction for a duration of %s minutes until %s \nThe extracted ADM will be POSTed as a parameter \"adm\" to %s", duration, time.Now().Add(durationTime).String(), horaAddr)))
}

func startRealtimeExtraction(duration time.Duration, horaAddr string) {
	starttime := time.Now()
	endtime := starttime.Add(duration)
	reader := &influxkieker.InfluxKiekerReader{
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
	m, ok := <-ch
	if ok {
		log.Print("Sending ADM to " + horaAddr)

		mjson, err := json.Marshal(m)
		if err != nil {
			log.Print("Error marshalling ADM")
			return
		}

		data := url.Values{}
		data.Set("adm", string(mjson))

		client := &http.Client{}
		r, _ := http.NewRequest("POST", horaAddr, bytes.NewBufferString(data.Encode())) // <-- URL-encoded payload
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

		resp, err := client.Do(r)
		if err != nil {
			log.Print("Error sending ADM", err)
			return
		}
		if resp == nil {
			log.Print("Error sending ADM: received empty response. Please check target address and port.")
			return
		}
		fmt.Println(resp.Status)
	}
}

func startBatchExtraction(starttime, endtime time.Time) {
	outdir := viper.GetString("admx.outdir")
	outdir += "-" + time.Now().Format("2006-01-02T15:04:05Z07:00")
	err := os.Mkdir(outdir, 0755)
	if err != nil {
		log.Printf("evaluator: cannot create outdir: %s. %s", outdir, err)
	}

	reader := &influxkieker.InfluxKiekerReader{
		Addr:      viper.GetString("influxdb.kieker.addr"),
		Username:  viper.GetString("influxdb.kieker.username"),
		Password:  viper.GetString("influxdb.kieker.password"),
		KiekerDb:  viper.GetString("influxdb.kieker.db"),
		Batch:     true,
		Starttime: starttime,
		Endtime:   endtime,
	}
	ch := reader.Read()
	m, ok := <-ch
	if ok {
		// Export json
		mjson, err := json.Marshal(m)
		if err != nil {
			log.Print("Error marshalling ADM")
			return
		}
		f, err := os.Create(outdir + "/adm.json")
		if err != nil {
			log.Printf("evaluator: cannot create file. %s", err)
		}
		defer f.Close()
		_, err = f.Write(mjson)
		if err != nil {
			log.Printf("evaluator: cannot write to file. %s", err)
		}
		// Export dot
		dotStr := "digraph {"
		for component, depInfo := range m {
			for _, dep := range depInfo.Dependencies {
				dotStr += component + "->" + dep.Callee.UniqName() + "\n"
			}
		}
		dotStr += "}"
		fdot, err := os.Create(outdir + "/adm.dot")
		if err != nil {
			log.Printf("evaluator: cannot create file. %s", err)
		}
		defer fdot.Close()
		_, err = fdot.WriteString(dotStr)
		if err != nil {
			log.Printf("evaluator: cannot write to file. %s", err)
		}
	}
}
