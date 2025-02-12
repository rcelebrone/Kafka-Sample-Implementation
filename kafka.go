package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Evento struct {
	Evento string  `json:"evento"`
	ID     int     `json:"id"`
	Valor  float64 `json:"valor"`
}

func main() {
	// Produtor
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "mms.order",
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	evento := Evento{
		Evento: "mms.order.created",
		ID:     123,
		Valor:  99.90,
	}

	eventoJSON, _ := json.Marshal(evento)

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: eventoJSON,
		},
	)
	if err != nil {
		log.Fatal("Falhou ao escrever mensagem:", err)
	}

	// Consumidor
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "mms.order",
		GroupID:  "meu_grupo",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Falhou ao ler mensagem:", err)
		}

		var evento Evento
		json.Unmarshal(m.Value, &evento)

		fmt.Printf("mensagem: %+v\n", evento)
	}
}
