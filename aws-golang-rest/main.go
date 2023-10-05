package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"

	tooling "aws-golang-rest/tooling"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var unames []string

const tableName = "go-dynamodb-reference-table"

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile | log.LUTC)
	log.Println("starting")
	port := 8000
	log.Printf("creating DynaomDB client for DynamoDB Local running on port %d\n", port)
	dynamodbClient := tooling.CreateLocalClient(port)
	log.Printf("creating table '%s' if it does not already exist\n", tableName)
	didCreateTable := tooling.CreateTableIfNotExists(dynamodbClient, tableName)
	log.Printf("did create table '%s'? %v\n", tableName, didCreateTable)
	log.Println("running conditional check failure example")
	putItemConditionCheckFailureExample(dynamodbClient)
	log.Println("running seed items example")
	seedItems(dynamodbClient)
	log.Println("running delete all items example")
	updateAllItemsConcurrently(dynamodbClient)
	log.Println("running delete all items example")
	deleteAllItems(dynamodbClient, unames)
	log.Println("completed")
}

// Performs a PutItem with the same item twice. The second time fails with a conditional check failure.
func putItemConditionCheckFailureExample(dynamodbClient *dynamodb.Client) {
	item := struct {
		PK string `dynamodbav:"PK"`
		SK string `dynamodbav:"SK"`
	}{
		PK: "ITEM#123",
		SK: "A",
	}
	ddbJson, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Fatal("failed to marshal item", err)
	}

	log.Printf("putting item %v\n", item)
	err = putItem(dynamodbClient, tableName, ddbJson)
	if err != nil {
		log.Fatal("PutItem failed", err)
	}

	log.Println("putting same item; this should fail with condition check failure")
	err = putItem(dynamodbClient, tableName, ddbJson)
	if err == nil {
		log.Fatal("expected duplicate PutItem request to fail with condition check failure, but it did not")
	}

	if tooling.IsConditionalCheckFailure(err) {
		log.Println("as expected: condition check failure error", err)
	} else {
		log.Println("unexpected error", err)
	}
}

func seedItems(dynamodbClient *dynamodb.Client) {
	tooling.CreateTableIfNotExists(dynamodbClient, tableName)
	numItems := 500
	for i := 0; i < numItems; i++ {
		item := map[string]types.AttributeValue{
			"PK":            &types.AttributeValueMemberS{Value: "PK-" + strconv.Itoa(i)},
			"SK":            &types.AttributeValueMemberS{Value: "A"},
			"RandomContent": &types.AttributeValueMemberS{Value: createRandomTextContent(10000)},
		}
		err := putItem(dynamodbClient, tableName, item)
		if err != nil {
			log.Fatal("failed to put item", err)
		}
	}
}

func createRandomTextContent(n int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func putItem(d *dynamodb.Client, tableName string, item map[string]types.AttributeValue) error {
	_, err := d.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	return err
}

func deleteAllItems(dynamodbClient *dynamodb.Client, unames []string) {
	err := tooling.DeleteAllItems(dynamodbClient, tableName, unames)
	if err != nil {
		log.Fatal("failed to delete all items", err)
	}
}

func updateAllItems(dynamodbClient *dynamodb.Client, uname string) {
	err := tooling.UpdateAllItems(dynamodbClient, tableName, uname)
	if err != nil {
		log.Fatal("failed to delete all items", err)
	}
}

func worker(id int, dynamodbClient *dynamodb.Client, uname string, wg *sync.WaitGroup) {
    defer wg.Done() // Decrement the WaitGroup counter when the function exits
    fmt.Printf("Worker %d started\n", id)
	updateAllItems(dynamodbClient, uname)
    fmt.Printf("Worker %d finished\n", id)
}


func updateAllItemsConcurrently(dynamodbClient *dynamodb.Client){
	var wg sync.WaitGroup

    numWorkers := 300

    for i := 0; i < numWorkers; i++ {
        wg.Add(1) // Increment the WaitGroup counter for each worker
		uname := tooling.GetRandomName(1)
		unames = append(unames, uname)
        go worker(i, dynamodbClient, uname, &wg)
    }

    // Wait for all workers to finish
    wg.Wait()

    fmt.Println("All workers have finished")
}
