package main

import (

	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	"math/rand"

	"log"
	"net/http"
	"strconv"
)


//Document struct
type Document struct {
	ID   string
	Name string
	Size int64
}


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}


func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/documents", writeRequestCreateToBroker).Methods("GET")
	//router.HandleFunc("/documents/{key}", getDocumentsById).Methods("GET")
	//router.HandleFunc("/documents", saveFileByData).Methods("POST")
	//router.HandleFunc("/documents/{key}", deleteDocumentsById).Methods("DELETE")


	log.Fatal(http.ListenAndServe(":9000", router))
}


func writeRequestCreateToBroker(w http.ResponseWriter, r *http.Request){
	res, err := writeOnQueue()
	failOnError(err, "Failed to handle RPC request")
	log.Printf(" [.] Got %d", res)
}



func writeOnQueue() (res int, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	err = ch.Publish(
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(strconv.Itoa(1)),
		})
	failOnError(err, "Failed to publish a message")
	for d := range msgs {
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")
			break
		}
	}
	return
}