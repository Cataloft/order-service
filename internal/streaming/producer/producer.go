package producer

import (
	"fmt"
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
	//wg := sync.WaitGroup{}
	//
	//wg.Add(len(sliceGenData))
	fmt.Println(len(sliceGenData))
	for _, genData := range sliceGenData {
		err := c.conn.Publish(subject, []byte(genData))
		if err != nil {
			log.Fatal(err)

		}
	}
	//wg.Wait()
	//time.Sleep(1000 * time.Millisecond)

	err := c.conn.Close()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
