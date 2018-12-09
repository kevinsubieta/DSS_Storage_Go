package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"

	"github.com/streadway/amqp"
)

var TYPE_READ_ALL_DOCUMENTS = 0
var TYPE_SAVE_DOCUMENT = 1
var TYPE_DELETE_DOCUMENT = 2
var CORR_ID string

type Protocol struct {
	Name    string
	Body    string
	Content []byte
	Status  bool
	Type    int
}


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

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/documents", createRequestToListFiles).Methods("GET")
	router.HandleFunc("/documents", createRequestToQueueForSaveFile).Methods("POST")
	router.HandleFunc("/documents/{key}", createRequestToQueueForDeleteAnyFileById).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":9000", router))

}

func openConnectionToQueue() *amqp.Channel {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	return ch
}

func readValuesFromQueue(w http.ResponseWriter) {
	CORR_ID = randomString(32)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	q, err := channel.QueueDeclare(
		"StorageResponses", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	failOnError(err, "Failed to set QoS"+q.Name)

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

		for d := range msgs {
			//if CORR_ID == d.CorrelationId {
				res := d.Body
				var messageFromQueue Protocol
				json.Unmarshal(res, &messageFromQueue)
				returnValueToUser(w, messageFromQueue)
				d.Ack(false)
				break
			//}
		}
	log.Printf("Reading queue from responses")
}

func writeRequestOnQueue(messageRequest []byte) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	storageRequestqueue, err := channel.QueueDeclare(
		"StorageRequest", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = channel.Publish(
		"",                       // exchange
		storageRequestqueue.Name, // routing key
		false,                    // mandatory
		false,                    // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: CORR_ID,
			Body:          messageRequest,
		})
	failOnError(err, "Failed to publish a message")
}

func returnValueToUser(w http.ResponseWriter, message Protocol) {
	if message.Type == TYPE_READ_ALL_DOCUMENTS {
		if message.Status {
			listOfFilesJson :=  message.Body
			var listOfFiles []Document
			json.Unmarshal([]byte(listOfFilesJson), &listOfFiles)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(listOfFiles)
		} else {
			http.Error(w, "Error on creating file", http.StatusBadRequest)
		}
	}else if message.Type == TYPE_SAVE_DOCUMENT {
		if message.Status {
			http.Error(w, "File created", http.StatusOK)
		} else {
			http.Error(w, "Error on creating file", http.StatusBadRequest)
		}

	} else if message.Type == TYPE_DELETE_DOCUMENT {
		if message.Status {
			http.Error(w, "File deleted", http.StatusOK)
		} else {
			http.Error(w, "Error on deleting file", http.StatusBadRequest)
		}
	}
}


func createRequestToListFiles(w http.ResponseWriter, r *http.Request) {
	var listAllRequest = Protocol{"", "", nil, true, TYPE_READ_ALL_DOCUMENTS}
	requestInJson, _ := json.Marshal(listAllRequest)
	writeRequestOnQueue(requestInJson)
	readValuesFromQueue(w)
}

func createRequestToQueueForSaveFile(w http.ResponseWriter, r *http.Request) {
	var Buf bytes.Buffer
	// in your case file would be fileupload
	file, header, err := r.FormFile("file")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	name := strings.Split(header.Filename, ".")
	fmt.Printf("File name %s\n", name[0])
	io.Copy(&Buf, file)
	contents := Buf.Bytes()
	Buf.Reset()

	//var channel = openConnectionToQueue()

	var fileToSaveRequest = Protocol{header.Filename, string(contents), contents, true, TYPE_SAVE_DOCUMENT}
	requestInJson, err := json.Marshal(fileToSaveRequest)
	writeRequestOnQueue(requestInJson)
	readValuesFromQueue(w)
}


func createRequestToQueueForDeleteAnyFileById(w http.ResponseWriter, r *http.Request) {
	var idFile = r.URL.Path[11:]
	var fileToDeleteRequest = Protocol{idFile, string(idFile), nil, true, TYPE_DELETE_DOCUMENT}
	requestInJson, _ := json.Marshal(fileToDeleteRequest)
	writeRequestOnQueue(requestInJson)
	readValuesFromQueue(w)
}
