<?php
declare(strict_types=1);

namespace Tests\Unit;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Sdk;
use PDA\PDA;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

class PDATest extends TestCase
{
    private $pda;
    private $dynamoDb;

    public function setUp():void
    {
        $this->dynamoDb = (new Sdk([
                                   'endpoint'   => 'http://localhost:8000',
                                   'region'   => 'ap-south-1',
                                   'version'  => 'latest'
                               ]))->createDynamoDb();
        $this->createTables();
        echo "\n";
        $this->pda = new PDA($this->dynamoDb);
    }

    public function tearDown():void
    {
        $this->deleteTables();
        echo "\n";
    }

    public function createTables(): void
    {

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
            echo "\nCreated table: {$categories['TableName']}";
            $result = $this->dynamoDb->createTable($products);
            echo "\nCreated table: {$products['TableName']}";
        } catch (DynamoDbException $DynamoDbException) {
            echo "\nUnable to create tables";
            echo $DynamoDbException->getMessage() . "\n";
        }
    }

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
            echo "\nDeleted table : {$categories['TableName']}";
            $this->dynamoDb->deleteTable($products);
            echo "\nDeleted table : {$products['TableName']}";
        } catch (DynamoDbException $e) {
            echo "\nUnable to delete all tables";
            echo $e->getMessage() . "\n";
        }
    }

    public function insertCategorySuccessData(): array
    {
        return [
            [
                'categories',
                ['id', 'name', 'status'],
                [[Uuid::uuid4()->toString(), 'fruits', 1]]
            ],
            [
                'categories',
                ['id', 'name', 'status'],
                [
                    [Uuid::uuid4()->toString(), 'phones', 0],
                    [Uuid::uuid4()->toString(), 'fruits', 1],
                    [Uuid::uuid4()->toString(), 'vegetables', 0],
                    [Uuid::uuid4()->toString(), 'books', 1]
                ]
            ]
        ];
    }

    /**
     * @dataProvider insertCategorySuccessData
     * @param String $table
     * @param Array $columns
     * @param Array $values
     */
    public function testInsertCategorySuccess($table, $columns, $values): void
    {
        $this->assertEquals(
            '',
            $this->pda->insert(
                $table,
                $columns,
                $values
            )
        );
    }

    public function insertCategoryFailingData(): array
    {
        return [
            [
                '',
                ['id', 'name', 'status'],
                [[Uuid::uuid4()->toString(), 'fruits', 1]]
            ],
            [
                'categories',
                ['id', 'name'],
                [[Uuid::uuid4()->toString(), 'fruits', 1]]
            ],
            [
                'categories',
                ['id', 'name', 'status'],
                [[Uuid::uuid4()->toString(), 'fruits']]
            ],
            [
                'categories',
                ['id', 'name', 'status'],
                [[Uuid::uuid4()->toString(), 1, 1]]
            ]
        ];
    }

    /**
     * @dataProvider insertCategoryFailingData
     * @param String $table
     * @param Array $columns
     * @param Array $values
     */
    public function testInsertCategoryFailing($table, $columns, $values): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->assertEquals(
            '',
            $this->pda->insert($table, $columns, $values)
        );
    }

    /*
    public function testSelectAllColumns()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT * FROM products',
            $pda->select('products')
        );
    }

    public function testSelectSpecificColumns()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT id, name FROM products',
            $pda->select('products', ['id', 'name'])
        );
    }

    public function testSelectWithOrderBySingleColumn()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC',
            $pda->select('products', ['id', 'name'], [['id', 'desc']])
        );
    }

    public function testSelectWithOrderByMultipleColumn()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC, name ASC',
            $pda->select('products', ['id', 'name'], [['id', 'desc'], ['name', 'asc']])
        );
    }

    public function testSelectWithCapitalizedKeywords()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC',
            $pda->select('products', ['id', 'name'], [['id', 'desc']])
        );
    }

    public function testSelectWithLimit()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT * FROM products LIMIT 10',
            $pda->select('products', [], [], [10])
        );
    }

    public function testSelectWithLimitAndOffset()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT * FROM products LIMIT ALL OFFSET 10',
            $pda->select('products', [], [], [null, 10])
        );
    }

    public function testSelectCount()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT *, COUNT("id") FROM products',
            $pda->select('products', [], [], [], ["count", "id", "*"])
        );
    }

    public function testSelectAgg()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT MAX("cost") FROM products',
            $pda->select('products', [], [], [], ["max", "cost"])
        );
    }

    public function testSelectGroupBy()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT MAX("cost") FROM products GROUP BY cost',
            $pda->select('products', [], [], [], ["max", "cost"], ["cost"])
        );
    }


    public function testSelectDistinct()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT DISTINCT name FROM products',
            $pda->select('products', [], [], [], [], [], ["name"])
        );
    }

    public function testSelectJoin()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT * FROM products JOIN categories ON products.category_id = categories.id',
            $pda->select('products', [], [], [], [], [], [], ["categories", "category_id", "id"])
        );
    }

    public function testSelectJoinWithDefaultAsId()
    {
        $pda = new PDA();
        $this->assertEquals(
            'SELECT * FROM products JOIN categories ON products.category_id = categories.id',
            $pda->select('products', [], [], [], [], [], [], ["categories", "category_id"])
        );
    }
    */
}
