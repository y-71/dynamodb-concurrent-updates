package tooling

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/smithy-go"
)

// CreateLocalClient Creates a local DynamoDb Client on the specified port. Useful for connecting to DynamoDB Local or
// LocalStack.
func CreateLocalClient(port int) *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{})(aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%d", port)}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func tableExists(d *dynamodb.Client, name string) bool {
	tables, err := d.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("ListTables failed", err)
	}
	for _, n := range tables.TableNames {
		if n == name {
			return true
		}
	}
	return false
}

// CreateTableIfNotExists If the table does not exist, creates it and returns true. Otherwise, does nothing and returns false.
func CreateTableIfNotExists(d *dynamodb.Client, tableName string) bool {
	if tableExists(d, tableName) {
		return false
	}
	_, err := d.CreateTable(context.TODO(), buildCreateTableInput(tableName))
	if err != nil {
		log.Fatal("CreateTable failed", err)
	}
	return true
}

func buildCreateTableInput(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	}
}

// IsConditionalCheckFailure Returns true if the error is a ConditionalCheckFailedException, else returns false.
// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
func IsConditionalCheckFailure(err error) bool {
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		return true
	}
	var oe *smithy.OperationError
	if errors.As(err, &oe) {
		var re *http.ResponseError
		if errors.As(err, &re) {
			var tce *types.TransactionCanceledException
			if errors.As(err, &tce) {
				for _, reason := range tce.CancellationReasons {
					if *reason.Code == "ConditionalCheckFailed" {
						return true
					}
				}
			}
		}
	}

	return false
}

// DeleteAllItems Deletes all the items in the table
func DeleteAllItems(d *dynamodb.Client, tableName string, unames []string) error {
	var offset map[string]types.AttributeValue
	for {
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		}
		if offset != nil {
			scanInput.ExclusiveStartKey = offset
		}
		result, err := d.Scan(context.TODO(), scanInput)
		if err != nil {
			return err
		}
		
		for _, item := range result.Items {
			
			// extract all of the keys from the item
			keys := make([]string, 0, len(item))
			for k := range item {
				keys = append(keys, k)
			}
			var subset StringSlice = unames

			if (!subset.IsSubset(keys)){
				fmt.Println(item)
				log.Panic("issues with concurrency")
			}


			_, err := d.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
				TableName: aws.String(tableName),
				Key:       map[string]types.AttributeValue{"PK": item["PK"], "SK": item["SK"]},
			},
			)
			if err != nil {
				return err
			}
		}

		if result.LastEvaluatedKey == nil {
			break
		}
		offset = result.LastEvaluatedKey
	}
	return nil

}


func UpdateAllItems(d *dynamodb.Client, tableName, uname string) error {
	var offset map[string]types.AttributeValue
	for {
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		}
		if offset != nil {
			scanInput.ExclusiveStartKey = offset
		}
		result, err := d.Scan(context.TODO(), scanInput)
		if err != nil {
			return err
		}
		
		ShuffleArray(result.Items)
		
		for _, item := range result.Items {
			_, err := d.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":r": &types.AttributeValueMemberS{Value: "filled"},
				},
				TableName: aws.String(tableName),

				Key:       map[string]types.AttributeValue{"PK": item["PK"], "SK": item["SK"]},
				// ReturnValues: String("UPDATED_NEW"),
    			UpdateExpression: aws.String(fmt.Sprintf("set %s = :r", uname)),
			},)
			if err != nil {
				log.Fatalf("Got error calling UpdateItem: %s", err)
			}
		}

		if result.LastEvaluatedKey == nil {
			break
		}
		offset = result.LastEvaluatedKey
	}
	return nil

}

// ShuffleArray shuffles an array of any type using the Fisher-Yates algorithm.
func ShuffleArray(arr interface{}) {
	swap := reflect.Swapper(arr)
	length := reflect.ValueOf(arr).Len()

	if length <= 1 {
		return
	}

	for i := length - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}