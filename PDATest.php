<?php

namespace Tests\Unit;

use App\Components\Sql;

class PDATest extends PHPUnit_Framework_TestCase
{

    /**
     * A basic test example.
     *
     * @return void
     */
    public function testRun()
    {
        $this->assertTrue(true);
    }

    public function testSelectAllColumns()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT * FROM products',
            $sql->select('products')
        );
    }

    public function testSelectSpecificColumns()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT id, name FROM products',
            $sql->select('products', ['id', 'name'])
        );
    }

    public function testSelectWithOrderBySingleColumn()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC',
            $sql->select('products', ['id', 'name'], [['id', 'desc']])
        );
    }

    public function testSelectWithOrderByMultipleColumn()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC, name ASC',
            $sql->select('products', ['id', 'name'], [['id', 'desc'], ['name', 'asc']])
        );
    }

    public function testSelectWithCapitalizedKeywords()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT id, name FROM products ORDER BY id DESC',
            $sql->select('products', ['id', 'name'], [['id', 'desc']])
        );
    }

    public function testSelectWithLimit()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT * FROM products LIMIT 10',
            $sql->select('products', [], [], [10])
        );
    }

    public function testSelectWithLimitAndOffset()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT * FROM products LIMIT ALL OFFSET 10',
            $sql->select('products', [], [], [null, 10])
        );
    }

    public function testSelectCount()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT *, COUNT("id") FROM products',
            $sql->select('products', [], [], [], ["count", "id", "*"])
        );
    }

    public function testSelectAgg()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT MAX("cost") FROM products',
            $sql->select('products', [], [], [], ["max", "cost"])
        );
    }

    public function testSelectGroupBy()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT MAX("cost") FROM products GROUP BY cost',
            $sql->select('products', [], [], [], ["max", "cost"], ["cost"])
        );
    }


    public function testSelectDistinct()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT DISTINCT name FROM products',
            $sql->select('products', [], [], [], [], [], ["name"])
        );
    }

    public function testSelectJoin()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT * FROM products JOIN categories ON products.category_id = categories.id',
            $sql->select('products', [], [], [], [], [], [], ["categories", "category_id", "id"])
        );
    }

    public function testSelectJoinWithDefaultAsId()
    {
        $sql = new Sql();
        $this->assertEquals(
            'SELECT * FROM products JOIN categories ON products.category_id = categories.id',
            $sql->select('products', [], [], [], [], [], [], ["categories", "category_id"])
        );
    }

    public function testInsertOneRow()
    {
        $sql = new Sql();
        $this->assertEquals(
            'INSERT INTO products("id", "name", "cost", "color") VALUES (1, "apple", 100, "red")',
            $sql->insert('products', ["id", "name", "cost", "color"], [[1, "apple", 100, "red"]])
        );
    }

    public function testInsertMultipleRows()
    {
        $sql = new Sql();
        $this->assertEquals(
            'INSERT INTO products("id", "name", "cost", "color") VALUES (1, "apple", 100, "red"), (2, "orange", 50, "orange")',
            $sql->insert(
                'products',
                ["id", "name", "cost", "color"],
                [
                    [1, "apple", 100, "red"],
                    [2, "orange", 50, "orange"]
                ]
            )
        );
    }
}
