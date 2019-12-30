<?php
namespace PDA;

class PDA
{
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
