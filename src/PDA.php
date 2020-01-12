<?php
declare(strict_types=1);

namespace PDA;

require_once dirname(__DIR__) . '/vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use InvalidArgumentException;

class PDA
{
    /**
     * @var DynamoDbClient
     */
    private DynamoDbClient $dynamoDb;

    public function __construct(DynamoDbClient $dynamoDb)
    {
        $this->dynamoDb = $dynamoDb;
    }


    private function throwMeBro(string $message = 'Check for missing table name or mismatch of columns/values'):void
    {
        throw new InvalidArgumentException($message);
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
                $json = json_encode(array_combine($columns, $value), JSON_THROW_ON_ERROR);
            } catch (JsonException $jsonException) {
                $this->throwMeBro($jsonException->getMessage());
            }

            $marshaler = new Marshaler();
            $params['Item'] = $marshaler->marshalJson($json);

            try {
                $this->dynamoDb->putItem($params);
            } catch (DynamoDbException $DynamoDbException) {
                $this->throwMeBro($DynamoDbException->getMessage());
            }
        }
    }

    public function select(string $table, array $columns = [], array $values = []) :void
    {
        if ($table === '') {
            $this->throwMeBro();
        }
        $marshaler = new Marshaler();

        $params = [
            'TableName' => $table,
            'ProjectionExpression' => implode(', ', $columns)
        ];

        try {
            $result = $this->dynamodb->scan($params);

            foreach ($result['Items'] as $item) {
                $category = $marshaler->unmarshalItem($item);
                print_r($category);
            }
        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }

    }
}
