package repository

import (
	"context"
	"fmt"
	"log"
	"order_service/internal/cache"
	"order_service/internal/model"

	pgx "github.com/jackc/pgx/v5"
)

type Repository struct {
	Conn *pgx.Conn
}

func New(connStr string) *Repository {
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Ping(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	return &Repository{
		Conn: conn,
	}
}

func (r *Repository) InsertDelivery(data *model.OrderInfo) error {
	_, err := r.Conn.Exec(context.Background(),
		"insert into public.delivery (delivery_id, name, phone, zip, city, address, region, email) values ($1, $2, $3, $4, $5, $6, $7, $8)",
		&data.Delivery.DeliveryId, &data.Delivery.Name, &data.Delivery.Phone, &data.Delivery.Zip, &data.Delivery.City, &data.Delivery.Address, &data.Delivery.Region, &data.Delivery.Email)
	if err != nil {
		return fmt.Errorf("insert data to delivery error", err)
	}

	return nil
}

func (r *Repository) InsertOrderInfo(data *model.OrderInfo) error {
	_, err := r.Conn.Exec(context.Background(),
		"insert into public.order_info (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		data.OrderUid, data.TrackNumber, data.Entry, data.Locale, data.InternalSignature, data.Delivery.DeliveryId, data.DeliveryService, data.Shardkey, data.SmId, data.DateCreated, data.OofShard)
	if err != nil {
		return fmt.Errorf("insert data to order_info error", err)
	}

	return nil
}

func (r *Repository) InsertPayment(data *model.OrderInfo) error {
	_, err := r.Conn.Exec(context.Background(),
		"insert into public.payment (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		data.OrderUid, data.Payment.RequestId, data.Payment.Currency, data.Payment.Provider, data.Payment.Amount, data.Payment.PaymentDt, data.Payment.Bank, data.Payment.DeliveryCost, data.Payment.GoodsTotal, data.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("insert data to payment error", err)
	}

	return nil
}

func (r *Repository) InsertItems(data *model.OrderInfo) error {
	for _, item := range data.Items {
		_, err := r.Conn.Exec(context.Background(),
			"insert into public.items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status, order_id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
			item.ChrtId, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.TotalPrice, item.NmId, item.Brand, item.Status, data.OrderUid)
		if err != nil {
			return fmt.Errorf("insert data to items error", err)
		}
	}

	return nil
}

func (r *Repository) RecoverCache(cache *cache.Cache) error {
	var delivery model.Delivery
	var orderInfo model.OrderInfo
	var payment model.Payment
	var items model.Items

	rows, err := r.Conn.Query(context.Background(),
		"select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard, name, phone, zip, city, address, region, email, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee, price, rid, name AS item_name, sale, size, total_price, nm_id, brand, status, chrt_id from order_info_join_view")

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(
			&orderInfo.OrderUid, &orderInfo.TrackNumber, &orderInfo.Entry, &orderInfo.Locale,
			&orderInfo.InternalSignature, &orderInfo.CustomerId, &orderInfo.DeliveryService,
			&orderInfo.Shardkey, &orderInfo.SmId, &orderInfo.DateCreated, &orderInfo.OofShard,
			&delivery.Name, &delivery.Phone, &delivery.Zip, &delivery.City, &delivery.Address,
			&delivery.Region, &delivery.Email,
			&payment.Transaction, &payment.RequestId, &payment.Currency, &payment.Provider,
			&payment.Amount, &payment.PaymentDt, &payment.Bank, &payment.DeliveryCost,
			&payment.GoodsTotal, &payment.CustomFee,
			&items.Price, &items.Rid, &items.Name, &items.Sale, &items.Size, &items.TotalPrice,
			&items.NmId, &items.Brand, &items.Status, &items.ChrtId)
		if err != nil {
			log.Printf("Scan error: %v", err)
			return err
		}

		delivery.DeliveryId = orderInfo.CustomerId
		items.TrackNumber = orderInfo.TrackNumber
		orderInfo.Delivery = delivery
		orderInfo.Payment = payment
		orderInfo.Items = []model.Items{items}

		if _, dec := cache.Get(orderInfo.OrderUid); !dec {
			log.Println("key already exists")
			continue
		}

		cache.Set(orderInfo.OrderUid, orderInfo)
	}

	return nil
}
