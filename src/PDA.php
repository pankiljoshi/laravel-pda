<?php
declare(strict_types=1);

namespace PDA;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use InvalidArgumentException;

class PDA
{
    /**
     * @var DynamoDbClient
     */
    private DynamoDbClient $_dynamoDb;
    private array $_reservedKeywords = [
        'name', 'status'
    ];

    public function __construct(DynamoDbClient $dynamoDb)
    {
        $this->_dynamoDb = $dynamoDb;
    }

    private function _isReservedKeyword(string $keyword): bool
    {
        if (in_array($keyword, $this->_reservedKeywords, false)) {
            return true;
        }
        return false;
    }

    private function throwMeBro(string $message = 'Check for missing table name or mismatch of columns/values'): void
    {
        throw new InvalidArgumentException($message);
    }

    public function insert(string $table, array $columns, array $values): void
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
                $json 
                    = json_encode(array_combine($columns, $value), JSON_THROW_ON_ERROR);
            } catch (\JsonException $jsonException) {
                $this->throwMeBro($jsonException->getMessage());
            }

            $marshaler = new Marshaler();
            $params['Item'] = $marshaler->marshalJson($json);

            try {
                $this->_dynamoDb->putItem($params);
            } catch (DynamoDbException $DynamoDbException) {
                $this->throwMeBro($DynamoDbException->getMessage());
            }
        }
    }

    public function select(string $table, array $columns = [], array $values = []): string
    {
        if ($table === '') {
            $this->throwMeBro();
        }

        $marshaler = new Marshaler();

        $aliases = [];
        $select = [];
        
        foreach ($columns as $column) {
            if ($this->_isReservedKeyword($column)) {
                $aliases["#$column"] = $column;
                $select["#$column"] = $column;
                continue;
            }

            $select[$column] = $column;
        }

        $params = [
            'TableName' => $table,
            'ProjectionExpression' => implode(', ', array_keys($select)),
            'ExpressionAttributeNames' => $aliases
        ];

        try {
            $result = $this->_dynamoDb->scan($params);
            $responseArray = [];

            foreach ($result['Items'] as $item) {
                $responseArrayItem = $marshaler->unmarshalItem($item);
                ksort($responseArrayItem);
                $responseArray[] = $responseArrayItem;
            }

            uasort(
                $responseArray, 
                static function ($a, $b) {
                    return $a['name'] <=> $b['name'];  
                }
            );

            $responseArray = array_values($responseArray);

            sort($responseArray);

            return json_encode($responseArray, JSON_THROW_ON_ERROR, 512);
        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }
    }
}
