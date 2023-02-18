package cache

import (
	"context"
	"fmt"
	"order_service/internal/model"
	"order_service/internal/repository"
	"sync"
	"time"
)

type Cache struct {
	sync.RWMutex
	items map[string]Item
}

type Item struct {
	Value   model.OrderInfo
	Created time.Time
}

func New() *Cache {
	items := make(map[string]Item)
	cache := Cache{
		items: items,
	}
	return &cache
}

func (c *Cache) Set(key string, value model.OrderInfo) {

	c.Lock()
	defer c.Unlock()
	fmt.Println("дата", value)
	c.items[key] = Item{
		Value:   value,
		Created: time.Now(),
	}

}

func (c *Cache) Get(key string) (any, bool) {

	c.RLock()

	defer c.RUnlock()

	item, found := c.items[key]
	if item.Value.OrderUid != "" && !found {
		return nil, false
	}

	return item.Value, true
}

//func (c *Cache) Delete(key string) error {
//
//	c.Lock()
//
//	defer c.Unlock()
//
//	if _, found := c.items[key]; !found {
//		return errors.New("key not found")
//	}
//
//	delete(c.items, key)
//
//	return nil
//}

func (c *Cache) RecoverCache(repo *repository.Repository) error {
	var delivery model.Delivery
	var orderInfo model.OrderInfo
	var payment model.Payment
	var items model.Items
	//var numRows int

	//repo.Conn.QueryRow(context.Background(), "select count(*) from public.order_info").Scan(&numRows)
	//if numRows == 0 {
	//	return errors.New("tables are empty, nothing to recover")
	//}
	//
	//orderInfoRec := make([]model.OrderInfo, numRows)
	//
	//for i := 1; i <= numRows; i++ {
	//	rows, err := repo.Conn.Query(context.Background(),
	//		"\"select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard, name, phone, zip, city, address, region, email, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee, payment_id, price, rid, name AS item_name, sale, size, total_price, nm_id, brand, status, chrt_id from order_info_join_view\"")
	//
	//	repo.Conn.QueryRow(context.Background(),
	//		"select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard, name, phone, zip, city, address, region, email, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee, payment_id, price, rid, name AS item_name, sale, size, total_price, nm_id, brand, status, chrt_id from order_info_join_view").Scan(
	//		&orderInfo.OrderUid, &orderInfo.TrackNumber, &orderInfo.Entry, &orderInfo.Locale, &delivery.City, &delivery.Address, &delivery.Region, &delivery.Email)
	//	//repo.Conn.QueryRow(context.Background(), "select transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee from public.payment").Scan(
	//	//	&payment.Transaction, &payment.RequestId, &payment.Currency, &payment.Provider, &payment.Amount, &payment.PaymentDt, &payment.Bank, &payment.DeliveryCost, &payment.GoodsTotal, &payment.CustomFee)
	//	//repo.Conn.QueryRow(context.Background(), "select chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status, order_id from public.items").Scan(
	//	//	&items.ChrtId, &items.TrackNumber, &items.Price, &items.Rid, &items.Name, &items.Sale, &items.Size, &items.TotalPrice, &items.NmId, &items.Brand, &items.Status, &orderInfo.OrderUid)
	//	//repo.Conn.QueryRow(context.Background(), "select order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard from public.order_info").Scan(
	//	//	&orderInfo.OrderUid, &orderInfo.TrackNumber, &orderInfo.Entry, &orderInfo.Locale, &orderInfo.InternalSignature, &orderInfo.CustomerId, &orderInfo.DeliveryService, &orderInfo.Shardkey, &orderInfo.SmId, &orderInfo.DateCreated, &orderInfo.OofShard)
	//
	//	orderInfo.Delivery = delivery
	//	orderInfo.Payment = payment
	//	orderInfo.Items = []model.Items{items}
	//	orderInfoRec = append(orderInfoRec, orderInfo)
	//
	//	if _, dec := c.Get(orderInfo.OrderUid); !dec {
	//		fmt.Println("key already exists")
	//		continue
	//	}
	//
	//	c.Set(orderInfo.OrderUid, orderInfoRec[i])
	//}

	rows, err := repo.Conn.Query(context.Background(),
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
			fmt.Printf("Scan error: %v", err)
			return err
		}

		delivery.DeliveryId = orderInfo.CustomerId
		items.TrackNumber = orderInfo.TrackNumber
		orderInfo.Delivery = delivery
		orderInfo.Payment = payment
		orderInfo.Items = []model.Items{items}

		if _, dec := c.Get(orderInfo.OrderUid); !dec {
			fmt.Println("key already exists")
			continue
		}

		c.Set(orderInfo.OrderUid, orderInfo)

	}
	fmt.Println("Кеш восстановлен")
	return nil
}
