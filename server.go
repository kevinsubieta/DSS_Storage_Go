package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"io/ioutil"
	"log"
	"os"
)

var TYPE_READ_ALL_DOCUMENTS = 0
var TYPE_SAVE_DOCUMENT = 1
var TYPE_DELETE_DOCUMENT = 2

type Document struct {
	ID   string
	Name string
	Size int64
}

type Protocol struct {
	Name    string
	Body    string
	Content []byte
	Status  bool
	Type    int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}


func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	storageRequestqueue, err := ch.QueueDeclare(
		"StorageRequest", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		storageRequestqueue.Name, // queue
		"",                       // consumer
		false,                    // auto-ack
		false,                    // exclusive
		false,                    // no-local
		false,                    // no-wait
		nil,                      // args
	)
	failOnError(err, "Failed to register a consumer")

	storageResponsesqueue, err := ch.QueueDeclare(
		"StorageResponses", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			res := d.Body
			var messageFromQueue Protocol
			json.Unmarshal(res, &messageFromQueue)
			var requestInJson []byte
			if messageFromQueue.Type == TYPE_READ_ALL_DOCUMENTS {
				groupOfFiles := getListOfDocuments()
				groupOfFilesInJson, err := json.Marshal(groupOfFiles)
				messageFromQueue.Body = string(groupOfFilesInJson)
				requestInJson, err = json.Marshal(messageFromQueue)
				failOnError(err, "Failed to publish a message")
			} else if messageFromQueue.Type == TYPE_SAVE_DOCUMENT {
				if !saveFileByData(messageFromQueue.Content, messageFromQueue.Name) {
					messageFromQueue.Status = false
				}
				requestInJson, err = json.Marshal(messageFromQueue)
				failOnError(err, "Failed to publish a message")
			} else if messageFromQueue.Type == TYPE_DELETE_DOCUMENT {
				if !deleteFileByHashMD5Id(messageFromQueue.Body) {
					messageFromQueue.Status = false
				}
				requestInJson, err = json.Marshal(messageFromQueue)
				failOnError(err, "Failed to publish a message")
			}
			d.Ack(false)
			err = ch.Publish(
				"",                         // exchange
				storageResponsesqueue.Name, // routing key
				false,                      // mandatory
				false,                      // immediate
				amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          requestInJson,
				})
		}
	}()
	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}

func getListOfDocuments() []Document {
	var docs []Document
	fileDir := "./FileToTest/"
	fileInfos, err := ioutil.ReadDir(fileDir)
	if err != nil {
		fmt.Println("Error in accessing directory:", err)
	}

	for _, file := range fileInfos {
		fileHash, err := hashFileToMD5CheckSum(fileDir + "/" + file.Name())
		if err == nil {
			docs = append(docs, Document{ID: fileHash, Name: file.Name(), Size: file.Size()})
		}
	}
	return docs
}

func deleteFileByHashMD5Id(idFile string) bool {
	fileDir := "./FileToTest/"
	fileInfos, err := ioutil.ReadDir(fileDir)
	if err != nil {
		fmt.Println("Error in accessing directory:", err)
	}

	for _, file := range fileInfos {
		fileHash, err := hashFileToMD5CheckSum(fileDir + "/" + file.Name())
		if err == nil {
			if fileHash == idFile {
				var nameFileToRemove = file.Name()
				os.Remove(fileDir + nameFileToRemove)
				return true
			}
		}
	}
	return false
}

func saveFileByData(fileBytes []byte, fileName string) bool {
	filePath := "./FileToTest/"+fileName

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := ioutil.WriteFile(filePath, fileBytes, 777)
		if err == nil {
			return true
		}
		return false
	} else {
		return true
	}
}

func hashFileToMD5CheckSum(filePath string) (string, error) {
	var returnMD5String string
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	returnMD5String = hex.EncodeToString(hashInBytes)
	return returnMD5String, nil
}
