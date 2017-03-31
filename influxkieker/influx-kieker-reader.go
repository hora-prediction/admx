package influxkieker

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/hora-prediction/admx/kieker"
	"github.com/hora-prediction/hora/adm"

	"github.com/golang-collections/collections/stack"
	"github.com/influxdata/influxdb/client/v2"
)

var connectors = []string{
	"public com.sun.jersey.api.client.ClientResponse com.sun.jersey.client.apache4.ApacheHttpClient4Handler.handle(com.sun.jersey.api.client.ClientRequest)",
	"private void com.sun.jersey.server.impl.application.WebApplicationImpl._handleRequest(com.sun.jersey.server.impl.application.WebApplicationContext, com.sun.jersey.spi.container.ContainerRequest, com.sun.jersey.spi.container.ContainerResponse)",
}

func isConnector(r kieker.OperationExecutionRecord) bool {
	for _, v := range connectors {
		if r.OperationSignature == v {
			return true
		}
	}
	return false
}

type InfluxKiekerReader struct {
	Addr      string
	Username  string
	Password  string
	KiekerDb  string
	K8sDb     string
	Batch     bool
	Starttime time.Time
	Endtime   time.Time
}

func (r *InfluxKiekerReader) Read() <-chan adm.ADM {
	mCh := make(chan adm.ADM, 10)
	clnt, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     r.Addr,
		Username: r.Username,
		Password: r.Password,
	})
	if err != nil {
		log.Fatal("Error: cannot create new influxdb client", err)
		close(mCh)
		return mCh
	}
	if r.Batch {
		log.Print("Reading monitoring data in batch mode")
		go r.readBatch(clnt, mCh)
	} else {
		log.Print("Reading monitoring data in realtime mode")
		go r.readRealtime(clnt, mCh)
	}
	return mCh
}

func (r *InfluxKiekerReader) readBatch(clnt client.Client, mCh chan adm.ADM) {
	traceIds := make(map[int64]kieker.OperationExecutionRecords)
	m := adm.ADM{}

	// Get first and last timestamp in influxdb
	var curtimestamp, firsttimestamp, lasttimestamp time.Time
	firsttimestamp, lasttimestamp = r.getFirstAndLastTimestamp(clnt)
	if firsttimestamp.IsZero() && lasttimestamp.IsZero() {
		log.Print("Error: cannot find monitoring data")
		return
	}
	// Get the larger starttime
	if r.Starttime.After(firsttimestamp) {
		curtimestamp = r.Starttime.Add(-time.Nanosecond)
	} else {
		curtimestamp = firsttimestamp.Add(-time.Nanosecond)
	}

LoopChunk: // Loop to get all data because InfluxDB return max. 10000 records by default
	for {
		cmd := "select * from OperationExecution where time > " + strconv.FormatInt(curtimestamp.UnixNano(), 10) + " and time <= " + strconv.FormatInt(lasttimestamp.UnixNano(), 10)
		q := client.Query{
			Command:  cmd,
			Database: r.KiekerDb,
		}
		response, err := clnt.Query(q)
		if err != nil {
			log.Fatal("Error: cannot query data with cmd=", cmd, err)
			break
		}
		if response.Error() != nil {
			log.Fatal("Error: bad response with cmd=", cmd, response.Error())
			break
		}
		res := response.Results

		if len(res[0].Series) == 0 {
			break // break if no more data is returned
		}
		// Parse time and response time
		for _, row := range res[0].Series[0].Values {
			t, err := time.Parse(time.RFC3339, row[0].(string))
			if err != nil {
				log.Fatal(err)
				break
			}

			if t.After(lasttimestamp) || (!r.Endtime.IsZero() && t.After(r.Endtime)) {
				break LoopChunk // break chunk loop if timestamp of current query result exceeds the lasttimestamp or the defined endtime
			}
			eoi, _ := row[1].(json.Number).Int64()
			ess, _ := row[2].(json.Number).Int64()
			hostname, _ := row[3].(string)
			operationSignature, _ := row[4].(string)
			responseTime, _ := row[5].(json.Number).Int64()
			sessionId, _ := row[6].(string)
			tin, _ := row[7].(json.Number).Int64()
			tout, _ := row[8].(json.Number).Int64()
			traceId, _ := row[9].(json.Number).Int64()

			record := kieker.OperationExecutionRecord{
				Eoi:                eoi,
				Ess:                ess,
				Hostname:           hostname,
				OperationSignature: operationSignature,
				ResponseTime:       responseTime,
				SessionId:          sessionId,
				Tin:                tin,
				Tout:               tout,
				TraceId:            traceId,
			}

			trace, ok := traceIds[record.TraceId]
			if !ok {
				var newTrace kieker.OperationExecutionRecords
				traceIds[record.TraceId] = newTrace
				trace = newTrace
			}
			trace = append(trace, record)
			traceIds[record.TraceId] = trace

			curtimestamp = t
		}
	}
LoopTraces:
	for _, v := range traceIds {
		sort.Sort(v)
		var eoi, ess int64 = -1, -1
		stk := stack.New()
		for _, callee := range v {
			if callee.Eoi != eoi+1 {
				continue LoopTraces
			}
			if callee.Ess > ess+1 {
				log.Print("Error: broken trace:")
				continue LoopTraces
			}
			for i := ess - callee.Ess; i >= 0; i-- {
				stk.Pop()
			}
			eoi = callee.Eoi
			ess = callee.Ess
			if stk.Len() > 0 {
				if !isConnector(callee) {
					tmpstk := stack.New()
					// pop until we find a non connector
					for {
						if !isConnector(stk.Peek().(kieker.OperationExecutionRecord)) {
							break
						}
						tmpstk.Push(stk.Pop())
					}
					caller := stk.Peek().(kieker.OperationExecutionRecord)
					incrementCount(m, caller, callee)
					// Repush tmpstk
					for tmpstk.Len() > 0 {
						stk.Push(tmpstk.Pop())
					}
				}
			} else {
				incrementCountEntryPoint(m, callee)
			}
			stk.Push(callee)
		}
	}
	m.ComputeProb()
	log.Print(&m)
	log.Print()

	mCh <- m
	close(mCh)
	return
}

func incrementCountEntryPoint(m adm.ADM, caller kieker.OperationExecutionRecord) {
	compCaller := adm.Component{
		Name:     caller.OperationSignature,
		Hostname: caller.Hostname,
		Type:     "responsetime",
		Called:   0,
	}
	if _, ok := m[compCaller.UniqName()]; !ok {
		m.AddDependency(nil, &compCaller)
		addDefaultHardwareDependency(m, compCaller)
	}
	m.IncrementCount(nil, &compCaller)
}

func incrementCount(m adm.ADM, caller, callee kieker.OperationExecutionRecord) {
	// increment caller or create if not already exists
	compCaller := adm.Component{
		Name:     caller.OperationSignature,
		Hostname: caller.Hostname,
		Type:     "responsetime",
		Called:   0,
	}
	compCallee := adm.Component{
		Name:     callee.OperationSignature,
		Hostname: callee.Hostname,
		Type:     "responsetime",
		Called:   0,
	}

	m.AddDependency(&compCaller, &compCallee)
	// TODO: check if they already exist before adding
	addDefaultHardwareDependency(m, compCaller)
	addDefaultHardwareDependency(m, compCallee)

	m.IncrementCount(&compCaller, &compCallee)
}

func addDefaultHardwareDependency(m adm.ADM, component adm.Component) {
	cpu := adm.Component{
		Name:     "cpu0",
		Hostname: component.Hostname,
		Type:     "cpu",
		Called:   1<<63 - 1,
	}
	m.AddDependency(&component, &cpu)
	memory := adm.Component{
		Name:     "memory0",
		Hostname: component.Hostname,
		Type:     "memory",
		Called:   1<<63 - 1,
	}
	m.AddDependency(&component, &memory)

	// Set dependency to max so that the weight is 1.0
	compDepInfo := m[component.UniqName()]
	cpuDep := compDepInfo.Dependencies[cpu.UniqName()]
	cpuDep.Called = 1<<63 - 1
	memoryDep := compDepInfo.Dependencies[memory.UniqName()]
	memoryDep.Called = 1<<63 - 1
}

func (r *InfluxKiekerReader) readRealtime(clnt client.Client, mCh chan adm.ADM) {
	defer close(mCh)
	traceIds := make(map[int64]kieker.OperationExecutionRecords)
	m := adm.ADM{}

	waitDuration := r.Endtime.Sub(r.Starttime)
	waitCh := time.After(waitDuration)
	log.Print("Waiting " + waitDuration.String() + " until " + r.Endtime.String())
	<-waitCh

	cmd := "select * from OperationExecution where time > " + strconv.FormatInt(r.Starttime.UnixNano(), 10)
	q := client.Query{
		Command:  cmd,
		Database: r.KiekerDb,
	}
	response, err := clnt.Query(q)
	if err != nil {
		log.Fatal("Error: cannot query data with cmd=", cmd, err)
		return
	}
	if response.Error() != nil {
		log.Fatal("Error: bad response with cmd=", cmd, response.Error())
		return
	}
	res := response.Results

	if len(res[0].Series) == 0 {
		return // break if no more data is returned
	}
	// Parse time and response time
	for _, row := range res[0].Series[0].Values {
		//log.Print("row=", row)
		_, err := time.Parse(time.RFC3339, row[0].(string))
		if err != nil {
			log.Fatal(err)
			continue
		}

		eoi, _ := row[1].(json.Number).Int64()
		ess, _ := row[2].(json.Number).Int64()
		hostname, _ := row[3].(string)
		operationSignature, _ := row[4].(string)
		responseTime, _ := row[5].(json.Number).Int64()
		sessionId, _ := row[6].(string)
		tin, _ := row[7].(json.Number).Int64()
		tout, _ := row[8].(json.Number).Int64()
		traceId, _ := row[9].(json.Number).Int64()

		record := kieker.OperationExecutionRecord{
			Eoi:                eoi,
			Ess:                ess,
			Hostname:           hostname,
			OperationSignature: operationSignature,
			ResponseTime:       responseTime,
			SessionId:          sessionId,
			Tin:                tin,
			Tout:               tout,
			TraceId:            traceId,
		}

		trace, ok := traceIds[record.TraceId]
		if !ok {
			var newTrace kieker.OperationExecutionRecords
			traceIds[record.TraceId] = newTrace
			trace = newTrace
		}
		trace = append(trace, record)
		traceIds[record.TraceId] = trace

	}

LoopTraces:
	for _, v := range traceIds {
		sort.Sort(v)
		var eoi, ess int64 = -1, -1
		stk := stack.New()
		for _, callee := range v {
			if callee.Eoi != eoi+1 {
				continue LoopTraces
			}
			if callee.Ess > ess+1 {
				log.Print("Error: broken trace:")
				continue LoopTraces
			}
			for i := ess - callee.Ess; i >= 0; i-- {
				stk.Pop()
			}
			eoi = callee.Eoi
			ess = callee.Ess
			if stk.Len() > 0 {
				if !isConnector(callee) {
					tmpstk := stack.New()
					// pop until we find a non connector
					for {
						if !isConnector(stk.Peek().(kieker.OperationExecutionRecord)) {
							break
						}
						tmpstk.Push(stk.Pop())
					}
					caller := stk.Peek().(kieker.OperationExecutionRecord)
					incrementCount(m, caller, callee)
					// Repush tmpstk
					for tmpstk.Len() > 0 {
						stk.Push(tmpstk.Pop())
					}
				}
			} else {
				incrementCountEntryPoint(m, callee)
			}
			stk.Push(callee)
		}
	}
	m.ComputeProb()
	mCh <- m

	return
}

func (r *InfluxKiekerReader) getFirstAndLastTimestamp(clnt client.Client) (time.Time, time.Time) {
	var firsttimestamp, lasttimestamp time.Time
	cmd := "select first(responseTime) from OperationExecution"
	q := client.Query{
		Command:  cmd,
		Database: r.KiekerDb,
	}
	response, err := clnt.Query(q)
	if err != nil {
		log.Fatal("Error: cannot query data with cmd=", cmd, err)
		return time.Unix(0, 0), time.Unix(0, 0) // TODO: get last timestamp
	}
	if response.Error() != nil {
		log.Fatal("Error: bad response with cmd=", cmd, response.Error())
		return time.Unix(0, 0), time.Unix(0, 0)
	}
	res := response.Results
	if len(res[0].Series) == 0 {
		log.Print("Error: cannot find first timestamp ", response.Error())
		return time.Unix(0, 0), time.Unix(0, 0)
	}
	firsttimestamp, err = time.Parse(time.RFC3339, res[0].Series[0].Values[0][0].(string))

	// TODO: query for different components
	cmd = "select last(responseTime) from OperationExecution"
	q = client.Query{
		Command:  cmd,
		Database: r.KiekerDb,
	}
	response, err = clnt.Query(q)
	if err != nil {
		log.Fatal("Error: cannot query data with cmd=", cmd, err)
		return time.Unix(0, 0), time.Unix(0, 0)
	}
	if response.Error() != nil {
		log.Fatal("Error: bad response with cmd=", cmd, response.Error())
		return time.Unix(0, 0), time.Unix(0, 0)
	}
	res = response.Results
	lasttimestamp, err = time.Parse(time.RFC3339, res[0].Series[0].Values[0][0].(string))

	return firsttimestamp, lasttimestamp
}
