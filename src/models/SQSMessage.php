<?php

namespace achertovsky\sqs\models;

use yii\base\Model;
use yii\helpers\ArrayHelper;

/**
 * docs: https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-sqs-2012-11-05.html#sendmessage
 * [
 * 'DelaySeconds' => <integer>,
 * 'MessageAttributes' => [
 *   '<String>' => [
 *       'BinaryListValues' => [<string || resource || Psr\Http\Message\StreamInterface>, ...],
 *       'BinaryValue' => <string || resource || Psr\Http\Message\StreamInterface>,
 *       'DataType' => '<string>', // REQUIRED
 *       'StringListValues' => ['<string>', ...],
 *       'StringValue' => '<string>',
 *   ],
 *   // ...
 * ],
 * 'MessageBody' => '<string>', // REQUIRED
 * 'MessageDeduplicationId' => '<string>',
 * 'MessageGroupId' => '<string>',
 * 'QueueUrl' => '<string>', // REQUIRED
 */
class SQSMessage extends Model
{
    /**
     * @var integer
     */
    public $DelaySeconds = 0;
    /**
     * @var array
     */
    public $MessageAttributes = [];
    /**
     * @var string
     */
    public $MessageBody = '';
    /**
     * @var string
     */
    public $MessageDeduplicationId = '';
    /**
     * @var string
     */
    public $MessageGroupId = '';
    /**
     * @var string
     */
    public $QueueUrl = '';

    /** @inheritDoc */
    public function rules()
    {
        return [
            [['MessageBody', 'QueueUrl'], 'required'],
            [['DelaySeconds', 'MessageAttributes', 'MessageDeduplicationId', 'MessageGroupId'], 'safe'],
        ];
    }

    /** @inheritDoc */
    public function scenarios()
    {
        return ArrayHelper::merge(
            parent::scenarios(),
            [
                'batch' => ['MessageBody', 'DelaySeconds', 'MessageAttributes', 'MessageDeduplicationId', 'MessageGroupId'],
            ]
        );
    }
}
