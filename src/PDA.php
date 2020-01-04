<?php
namespace PDA;

require_once dirname(__DIR__) . '/vendor/autoload.php';

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Sdk;
use Aws\DynamoDb\Marshaler;
use http\Exception;

class PDA
{
    private $dynamoDb;

    public function deleteTables(): void
    {
        $categories = [
            'TableName' => 'categories'
        ];
        $products = [
            'TableName' => 'products'
        ];

        try {
            $this->dynamoDb->deleteTable($categories);
            echo "\nDeleted table : {$categories['TableName']}\n";
            $this->dynamoDb->deleteTable($products);
            echo "\nDeleted table : {$products['TableName']}\n";
        } catch (DynamoDbException $e) {
            echo "\nUnable to delete all tables:\n";
            echo $e->getMessage() . "\n";
        }
    }

    public function createTables(): void
    {
        $sdk = new Sdk([
                           'endpoint'   => 'http://localhost:8000',
                           'region'   => 'ap-south-1',
                           'version'  => 'latest'
                       ]);
        $this->dynamoDb = $sdk->createDynamoDb();

        $categories = [
            'TableName' => 'categories',
            'KeySchema' => [
                [
                    'AttributeName' => 'id',
                    'KeyType' => 'HASH'
                ],
                [
                    'AttributeName' => 'name',
                    'KeyType' => 'RANGE'
                ],
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'id',
                    'AttributeType' => 'S'
                ],
                [
                    'AttributeName' => 'name',
                    'AttributeType' => 'S'
                ]
            ],
            'ProvisionedThroughput'=> [
                'ReadCapacityUnits'=> 2,
                'WriteCapacityUnits'=> 2
            ]
        ];
        $products = [
            'TableName' => 'products',
            'KeySchema' => [
                [
                    'AttributeName' => 'id',
                    'KeyType' => 'HASH'
                ],
                [
                    'AttributeName' => 'category_id',
                    'KeyType' => 'RANGE'
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'id',
                    'AttributeType' => 'N'
                ],
                [
                    'AttributeName' => 'category_id',
                    'AttributeType' => 'N'
                ]
            ],
            'ProvisionedThroughput'=> [
                'ReadCapacityUnits'=> 2,
                'WriteCapacityUnits'=> 2
            ]
        ];

        try {
            $result = $this->dynamoDb->createTable($categories);
            echo "\nCreated table: {$categories['TableName']}\nStatus: " .
                $result['TableDescription']['TableStatus'] ."\n";
            $result = $this->dynamoDb->createTable($products);
            echo "\nCreated table: {$products['TableName']}\nStatus: " .
                $result['TableDescription']['TableStatus'] ."\n";
        } catch (DynamoDbException $e) {
            echo "Unable to create table:\n";
            echo $e->getMessage() . "\n";
        }
    }

    public function insert(string $table, array $columns, array $values) :void
    {
        if ($table === '' || !(count($columns) === count($values[0]))) {
            throw new \InvalidArgumentException('Check for missing table name or mismatch of columns/values');
        }

        $json = json_encode(array_combine($columns, $values[0]));

        $marshaler = new Marshaler();
        $params = [
            'TableName' => $table,
            'Item' => $marshaler->marshalJson($json)
        ];

        try {
            $this->dynamoDb->putItem($params);
        } catch (DynamoDbException $e) {
            echo "Unable to add item:\n";
            echo $e->getMessage() . "\n";
        }
    }
}
