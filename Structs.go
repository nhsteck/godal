package godal

type Postgres struct {
	Host        string
	Port        string
	Dbname      string
	User        string
	Pass        string
	SSLMode     string
	MaxIdleConn int32
	MaxOpenConn int32
}
