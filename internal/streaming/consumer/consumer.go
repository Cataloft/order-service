package consumer

import (
	"encoding/json"
	"errors"
	"log"

	"order_service/internal/cache"
	"order_service/internal/model"
	"order_service/internal/repository"

	"github.com/nats-io/stan.go"
)

type Subscriber struct {
	conn  stan.Conn
	repo  *repository.Repository
	cache *cache.Cache
	sub   stan.Subscription
}

func New(clusterID, clientID, natsURL string, repo *repository.Repository, cache *cache.Cache) *Subscriber {
	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatal(err)
	}
	return &Subscriber{
		conn:  conn,
		repo:  repo,
		cache: cache,
	}
}

func (s *Subscriber) Subscribe(subject string) error {
	sub, err := s.conn.Subscribe(subject, func(m *stan.Msg) {
		data := unmarshalData(m)
		err := s.WriteDb(data)
		if err != nil {
			log.Println(err)
		}
	}, stan.DurableName("main"))
	if err != nil {
		log.Fatal(err)
	}

	s.sub = sub

	return nil
}

func (s *Subscriber) Close() error {
	err := s.sub.Unsubscribe()
	if err != nil {
		log.Fatal(err)
	}
	return s.sub.Close()
}

func unmarshalData(m *stan.Msg) *model.OrderInfo {
	var data model.OrderInfo

	err := json.Unmarshal(m.Data, &data)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(data)

	return &data
}

func (s *Subscriber) WriteDb(data *model.OrderInfo) error {
	if data == nil {
		return errors.New("got nil data")
	}

	if _, dec := s.cache.Get(data.OrderUid); !dec {
		return errors.New("key already exists")
	}

	s.cache.Set(data.OrderUid, *data)

	err := s.repo.InsertDelivery(data)
	if err != nil {
		log.Fatal(err)
	}

	err = s.repo.InsertOrderInfo(data)
	if err != nil {
		log.Fatal(err)
	}

	err = s.repo.InsertPayment(data)
	if err != nil {
		log.Fatal(err)
	}

	err = s.repo.InsertItems(data)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("success insertion of data")

	return nil
}
