<?php
namespace PDA;

require_once dirname(__DIR__) . '/vendor/autoload.php';

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Sdk;

class PDA
{
    public function createTable(): void
    {
        $sdk = new Sdk([
                           'endpoint'   => 'http://localhost:8000',
                           'region'   => 'ap-south-1',
                           'version'  => 'latest'
                       ]);
        $dynamodb = $sdk->createDynamoDb();

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
                    'AttributeType' => 'N'
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
            $result = $dynamodb->createTable($categories);
            echo 'Created table.  Status: ' .
                $result['TableDescription']['TableStatus'] ."\n";
            $result = $dynamodb->createTable($products);
            echo 'Created table.  Status: ' .
                $result['TableDescription']['TableStatus'] ."\n";

        } catch (DynamoDbException $e) {
            echo "Unable to create table:\n";
            echo $e->getMessage() . "\n";
        }
    }
    public function insert(string $table, array $columns, array $values) :string
    {
        $return = 'error';
        if ($table === '' || !(count($columns) === count($values[0]))) {
            return $return;
        }
        $return = 'success';
        return $return;
    }
}
