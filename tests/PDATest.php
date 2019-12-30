<?php
#declare(strict_types=1);

namespace Tests\Unit;

use PDA\PDA;
use PHPUnit\Framework\TestCase;

class PDATest extends TestCase
{
    public function testInsertOneRow()
    {
        $pda = new PDA();
        $this->assertEquals(
            'success',
            $pda->insert(
                'products',
                ['id', 'name', 'cost', 'color'],
                [[1, 'apple', 100, 'red']]
            )
        );
    }
}
