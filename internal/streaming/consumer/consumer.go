package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	s.sub.Unsubscribe()
	return s.sub.Close()
}

func unmarshalData(m *stan.Msg) *model.OrderInfo {
	fmt.Printf("received message: %s\n", m.Data)
	var data model.OrderInfo
	err := json.Unmarshal(m.Data, &data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	fmt.Println(data)

	return &data
}

func (s *Subscriber) WriteDb(data *model.OrderInfo) error {
	if data == nil {
		log.Println("got nil data")
		return nil
	}

	if _, dec := s.cache.Get(data.OrderUid); !dec {
		return errors.New("key already exists")
	}

	s.cache.Set(data.OrderUid, *data)
	fmt.Println("success caching data")

	_, err := s.repo.Conn.Exec(context.Background(),
		"insert into public.delivery (delivery_id, name, phone, zip, city, address, region, email) values ($1, $2, $3, $4, $5, $6, $7, $8)",
		&data.Delivery.DeliveryId, &data.Delivery.Name, &data.Delivery.Phone, &data.Delivery.Zip, &data.Delivery.City, &data.Delivery.Address, &data.Delivery.Region, &data.Delivery.Email)
	if err != nil {
		return fmt.Errorf("insert data to delivery error", err)
	}

	_, err = s.repo.Conn.Exec(context.Background(),
		"insert into public.order_info (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		data.OrderUid, data.TrackNumber, data.Entry, data.Locale, data.InternalSignature, data.Delivery.DeliveryId, data.DeliveryService, data.Shardkey, data.SmId, data.DateCreated, data.OofShard)
	if err != nil {
		return fmt.Errorf("insert data to order_info error", err)
	}

	_, err = s.repo.Conn.Exec(context.Background(),
		"insert into public.payment (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		data.OrderUid, data.Payment.RequestId, data.Payment.Currency, data.Payment.Provider, data.Payment.Amount, data.Payment.PaymentDt, data.Payment.Bank, data.Payment.DeliveryCost, data.Payment.GoodsTotal, data.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("insert data to payment error", err)
	}

	_, err = s.repo.Conn.Exec(context.Background(),
		"insert into public.items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status, order_id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
		data.Items[0].ChrtId, data.Items[0].TrackNumber, data.Items[0].Price, data.Items[0].Rid, data.Items[0].Name, data.Items[0].Sale, data.Items[0].Size, data.Items[0].TotalPrice, data.Items[0].NmId, data.Items[0].Brand, data.Items[0].Status, data.OrderUid)
	if err != nil {
		return fmt.Errorf("insert data to items error", err)
	}

	fmt.Println("success insertion of data")

	return nil
}

//func (s *Subscriber) GetItems() error {
//	var orderInfoRec model.OrderInfo
//	var lenRows int
//	s.repo.Conn.QueryRow(context.Background(), "select count(*) from public.order_info").Scan(&lenRows)
//	for i := 0; i < lenRows; i++ {
//		s.repo.Conn.QueryRow(context.Background(), "select delivery_id, name, phone, zip, city, address, region, email from public.delivery").Scan(
//			&orderInfoRec.Delivery.DeliveryId, &orderInfoRec.Delivery.Name, &orderInfoRec.Delivery.Phone, &orderInfoRec.Delivery.Zip, &orderInfoRec.Delivery.City, &orderInfoRec.Delivery.Address, &orderInfoRec.Delivery.Region, &orderInfoRec.Delivery.Email)
//		s.repo.Conn.QueryRow(context.Background(), "select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard from public.order_info").Scan(
//			&orderInfoRec.OrderUid, &orderInfoRec.TrackNumber, &orderInfoRec.Entry, &orderInfoRec.Locale, &orderInfoRec.InternalSignature, &orderInfoRec.CustomerId, &orderInfoRec.DeliveryService, &orderInfoRec.Shardkey, &orderInfoRec.SmId, &orderInfoRec.DateCreated, &orderInfoRec.OofShard)
//		s.repo.Conn.QueryRow(context.Background(), "select transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee from public.payment").Scan(
//			&orderInfoRec.Payment.Transaction, &orderInfoRec.Payment.RequestId, &orderInfoRec.Payment.Currency, &orderInfoRec.Payment.Provider, &orderInfoRec.Payment.Amount, &orderInfoRec.Payment.PaymentDt, &orderInfoRec.Payment.Bank, &orderInfoRec.Payment.DeliveryCost, &orderInfoRec.Payment.GoodsTotal, &orderInfoRec.Payment.CustomFee)
//		s.repo.Conn.QueryRow(context.Background(), "select chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status, order_id from public.items").Scan(
//			&orderInfoRec.Items[i].ChrtId, &orderInfoRec.Items[i].TrackNumber, &orderInfoRec.Items[i].Price, &orderInfoRec.Items[i].Rid, &orderInfoRec.Items[i].Name, &orderInfoRec.Items[i].Sale, &orderInfoRec.Items[i].Size, &orderInfoRec.Items[i].TotalPrice, &orderInfoRec.Items[i].NmId, &orderInfoRec.Items[i].Brand, &orderInfoRec.Items[i].Status, &orderInfoRec.OrderUid)
//	}
//
//	return nil
//}
