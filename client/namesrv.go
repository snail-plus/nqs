package client

type Namesrvs struct {
	srvs []string
}

func (receiver Namesrvs) FindBrokerAddrByName(brokenName string) string {
	// TODO 从name server获取
	return "localhost:8089"
}
