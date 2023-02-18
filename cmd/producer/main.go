package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"order_service/internal/model"
	"order_service/internal/streaming/producer"
	"strconv"
)

const n = 10

func main() {
	clusterID := "default"
	clientProd := "client-producer"

	prod := producer.New(clusterID, clientProd, "0.0.0.0:4222")

	genData := GenerateData(n)
	for i, v := range genData {
		fmt.Println("id = ", i, ": ", v)
	}

	err := prod.Publish("order", genData)
	if err != nil {
		log.Printf("Publish error", err)
	}
}

func GenerateData(n int) []string {
	payment := model.Payment{
		Transaction:  "b563feb7b2b84b6test",
		RequestId:    "",
		Currency:     "USD",
		Provider:     "wbpay",
		Amount:       1817,
		PaymentDt:    1637907727,
		Bank:         "alpha",
		DeliveryCost: 1500,
		GoodsTotal:   317,
		CustomFee:    0,
	}

	delivery := model.Delivery{
		Name:       "Test Testov",
		Phone:      "+9720000000",
		Zip:        "2639809",
		City:       "Kiryat Mozkin",
		Address:    "Ploshad Mira 15",
		Region:     "Kraiot",
		Email:      "test@gmail.com",
		DeliveryId: "test",
	}

	items := model.Items{
		ChrtId:      9934930,
		TrackNumber: "WBILMTESTTRACK",
		Price:       453,
		Rid:         "ab4219087a764ae0btest",
		Name:        "Mascaras",
		Sale:        30,
		Size:        "0",
		TotalPrice:  317,
		NmId:        2389212,
		Brand:       "Vivienne Sabo",
		Status:      202,
	}

	orderInfo := model.OrderInfo{
		OrderUid:          "b563feb7b2b84b6test",
		TrackNumber:       "WBILMTESTTRACK",
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "",
		CustomerId:        delivery.DeliveryId,
		DeliveryService:   "meest",
		Shardkey:          "9",
		SmId:              99,
		DateCreated:       "2021-11-26T06:22:19Z",
		OofShard:          "1",
		Payment:           payment,
		Delivery:          delivery,
		Items:             []model.Items{items},
	}

	var sliceOrderJson []string

	orderInfo.Delivery.DeliveryId += "0"

	for i := 0; i < n; i++ {
		orderInfo.Delivery.DeliveryId = ConvertStr(orderInfo.Delivery.DeliveryId, i)
		orderInfo.TrackNumber = ConvertStr(orderInfo.TrackNumber, i)
		orderInfo.OrderUid = RandStringBytes() + orderInfo.Delivery.DeliveryId
		orderInfoJson, _ := json.Marshal(orderInfo)
		sliceOrderJson = append(sliceOrderJson, string(orderInfoJson))
	}
	return sliceOrderJson
}

func ConvertStr(field string, i int) string {
	field = field[:len(field)-1]
	field += strconv.FormatInt(int64(i), 10)
	return field
}

const syms = "abcdefghijklmnopqrstuvwxyz1234567890"

func RandStringBytes() string {
	field := make([]byte, 15)
	for i := range field {
		field[i] = syms[rand.Intn(len(syms))]
	}
	return string(field)
}
