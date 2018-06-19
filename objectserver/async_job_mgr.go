package objectserver

type AsyncJob interface {
	GetMethod() string
	GetHeaders() map[string]string
	GetAccount() string
	GetContainer() string
	GetObject() string
}

type AsyncJobMgr interface {
	New(vars, headers map[string]string) AsyncJob

	Save(job AsyncJob) error

	Next(device string, policy int) AsyncJob

	Finish(job AsyncJob) error
}
