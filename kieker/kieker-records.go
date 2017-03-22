package kieker

import ()

type OperationExecutionRecord struct {
	Eoi                int64  `json:"eoi"`
	Ess                int64  `json:"ess"`
	Hostname           string `json:"hostname"`
	OperationSignature string `json:"operationExecution"`
	ResponseTime       int64  `json:"responseTime"`
	SessionId          string `json:"sessionId"`
	Tin                int64  `json:"tin"`
	Tout               int64  `json:"tout"`
	TraceId            int64  `json:"traceId"`
}

type OperationExecutionRecords []OperationExecutionRecord

func (r OperationExecutionRecords) Len() int {
	return len(r)
}

func (rs OperationExecutionRecords) Less(i, j int) bool {
	if rs[i].Eoi < rs[j].Eoi {
		return true
	} else if rs[i].Eoi > rs[j].Eoi {
		return false
	} else {
		if rs[i].Ess <= rs[j].Ess {
			return true
		}
		return false
	}
}

func (rs OperationExecutionRecords) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}
