<?php
declare(strict_types=1);

namespace Tests\Unit;

use PDA\PDA;
use PHPUnit\Framework\TestCase;

class PDATest extends TestCase
{
    private $pda;
    public function setUp():void
    {
        $this->pda = new PDA();
        $this->pda->createTable();
    }

    public function testInsertOneRow(): void
    {
        $this->assertEquals(
            'success',
            $this->pda->insert(
                'products',
                ['id', 'name', 'cost', 'color'],
                [[1, 'apple', 100, 'red']]
            )
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

    public function testInsertMultipleRows()
    {
        $pda = new PDA();
        $this->assertEquals(
            'INSERT INTO products("id", "name", "cost", "color") VALUES (1, "apple", 100, "red"), (2, "orange", 50, "orange")',
            $pda->insert(
                'products',
                ["id", "name", "cost", "color"],
                [
                    [1, "apple", 100, "red"],
                    [2, "orange", 50, "orange"]
                ]
            )
        );
    }
    */
}
