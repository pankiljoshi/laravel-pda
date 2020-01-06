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

    private function throwMeBro(string $message = 'Check for missing table name or mismatch of columns/values'):void
    {
        throw new \InvalidArgumentException($message);
    }

    public function insert(string $table, array $columns, array $values) :void
    {
        if ($table === '' || !count($columns) || !(count($columns) === count($values[0]))) {
            $this->throwMeBro();
        }

        $params = [
            'TableName' => $table
            ];
        foreach ($values as $value) {
            $json = '{}';
            try {
                $json = json_encode(array_combine($columns, $value), JSON_THROW_ON_ERROR, 512);
            } catch (JsonException $jsonException) {
                $this->throwMeBro($jsonException->getMessage());
            }

            $marshaler = new Marshaler();
            $params['Item'] = $marshaler->marshalJson($json);

            try {
                $this->dynamoDb->putItem($params);
            } catch (DynamoDbException $e) {
                $this->throwMeBro($e->getMessage());
            }
        }
    }
}
