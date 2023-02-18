package producer

import (
	"log"

	"github.com/nats-io/stan.go"
)

type Cluster struct {
	conn stan.Conn
}

func New(clusterID, clientID, natsURL string) *Cluster {
	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatal(err)
	}
	return &Cluster{
		conn: conn,
	}
}

func (c *Cluster) Publish(subject string, sliceGenData []string) error {
	for _, genData := range sliceGenData {
		err := c.conn.Publish(subject, []byte(genData))
		if err != nil {
			log.Fatal(err)
		}
	}

	err := c.conn.Close()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}
