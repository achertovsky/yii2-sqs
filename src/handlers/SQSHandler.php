<?php
?
namespace achertovsky\sqs\handlers;
?
use Yii;
use Aws\Sqs\SqsClient;
use yii\base\BaseObject;
use common\models\SQSMessage;
use Aws\Exception\AwsException;
?
/**
 * Docs:
 * https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-sqs-2012-11-05.html#sendmessagebatch
 * Contains main operations of SQS 
 */
class SQSHandler extends BaseObject
{
    /**
     * Max amount of messages that may be sent is 10, refer docs link #1
     */
    const MAX_SEND_MESSAGE_BATCH = 10;
?
    /**
     * Config predefine
     *
     * @var array
     */
    protected $config = [
        'version' => "2012-11-05",
        'credentials' => false,
    ];
?
    /**
     * @var SqsClient
     */
    protected $client = null;
?
    /** @inheritDoc */
    public function init()
    {
        $this->client = new SqsClient($this->config);
    }
?
    /**
     * @param string $endpoint
     * @return void
     */
    public function setEndpoint($endpoint)
    {
        $this->config['endpoint'] = $endpoint;
        if (!is_null($this->client)) {
            $this->client = new SqsClient($this->config);
        }
    }
?
    /**
     * @param string $region
     * @return void
     */
    public function setRegion($region)
    {
        $this->config['region'] = $region;
        if (!is_null($this->client)) {
            $this->client = new SqsClient($this->config);
        }
    }
?
    /**
     * @param string $version
     * @return void
     */
    public function setVersion($version)
    {
        $this->config['version'] = $version;
        if (!is_null($this->client)) {
            $this->client = new SqsClient($this->config);
        }
    }
?
    /**
     * Adding to client credentials
     *
     * @param string $key
     * @param string $secret
     * @param string $token
     * @return void
     */
    public function setCredentials($key, $secret, $token = null)
    {
        $this->config['credentials'] = [
            'key' => $key,
            'secret' => $secret,
        ];
        if (!is_null($token)) {
            $this->config['credentials']['token'] = $token;
        }
        if (!is_null($this->client)) {
            $this->client = new SqsClient($this->config);
        }
    }
?
    /**
     * Sends an message to sqs in async manner
     *
     * @param SQSMessage $message
     * @return boolean
     */
    public function sendMessageAsync($message)
    {
        $this->send($message, 'sendMessageAsync');
    }
?
    /**
     * Sends an message to sqs
     *
     * @param SQSMessage $message
     * @return boolean
     */
    public function sendMessage($message)
    {
        $this->send($message, 'sendMessage');
    }
    
    /**
     * Sends an message to sqs
     *
     * @param SQSMessage[] $messages, pass by link
     * @return int items successfully sent
     */
    public function sendMessageBatch(&$messages)
    {
        if (empty($this->config['endpoint'])) {
            Yii::error("No endpoint set");
            return;
        }
?
        $send = [
            'Entries' => [],
            'QueueUrl' => $this->config['endpoint'],
        ];
?
        foreach ($messages as $message) {
            $message->scenario = 'batch';
            if (!$message->validate()) {
                Yii::error($message->errors);
                continue;
            }
            $entry = [
                'Id' => uniqid(),
            ];
            foreach ($message->getAttributes() as $name => $data) {
                if (empty($data)) {
                    continue;
                }
                $entry[$name] = $data;
            }
            $send['Entries'][] = $entry;
        }
        if (empty($send['Entries'])) {
            return 0;
        }
        
        $chunks = array_chunk($send['Entries'], 10);
?
        $count = 0;
        foreach ($chunks as $chunk) {
            try {
                $count += count($chunk);
                $send['Entries'] = $chunk;
                $this->client->sendMessageBatch($send);
            } catch (\Exception $ex) {
                Yii::error('SQSHandler sendMessageBatch Exception: Code: '.$ex->getCode().' ; Message: '.$ex->getMessage().' ; Trace: '.$ex->getTraceAsString());
                return $count;
            }
        }
?
        Yii::info("Sent to SQS $count");
        return $count;
    }
?
    /**
     * Contains logic of send message options
     *
     * @param SQSMessage $message
     * @param string $clientFunc
     * @return boolean
     */
    public function send($message, $clientFunc)
    {
        if (empty($message->QueueUrl) && !empty($this->endPoint)) {
            $message->QueueUrl = $this->endPoint;
        }
        if (!$message->validate()) {
            return false;
        }
        $send = [];
        foreach ($message->getAttributes() as $name => $data) {
            if (empty($data)) {
                continue;
            }
            $send[$name] = $data;
        }
        try {
            $this->client->$clientFunc($send);
        } catch (\Exception $ex) {
            Yii::error('SQSHandler send Exception: Code: '.$ex->getCode().' ; Message: '.$ex->getMessage().' ; Trace: '.$ex->getTraceAsString());
            return false;
        }
        return true;
    }
?
    /**
     * Receives messages
     *
     * @param array $params
     * @return array
     */
    public function receiveMessage($params)
    {
        if (empty($params['QueueUrl'])) {
            $params['QueueUrl'] = $this->config['endpoint'];
        }
        
        try {
            $messages = $this->client->receiveMessage($params);
            return $messages;
        } catch (\Exception $ex) {
            Yii::error('SQSHandler receiveMessage Exception: Code: '.$ex->getCode().' ; Message: '.$ex->getMessage().' ; Trace: '.$ex->getTraceAsString());
            return [];
        }
    }
?
    /**
     * Deletes messages
     *
     * @param array $params
     * @return boolean
     */
    public function deleteMessageBatch($params)
    {
        if (empty($params['QueueUrl'])) {
            $params['QueueUrl'] = $this->config['endpoint'];
        }
        
        try {
            $this->client->deleteMessageBatch($params);
        } catch (\Exception $ex) {
            Yii::error('SQSHandler deleteMessageBatch Exception: Code: '.$ex->getCode().' ; Message: '.$ex->getMessage().' ; Trace: '.$ex->getTraceAsString());
            return false;
        }
        return true;
    }
?
    /**
     * Deletes messages
     *
     * @param array $params
     * @return boolean
     */
    public function deleteMessageBatchAsync($params)
    {
        if (empty($params['QueueUrl'])) {
            $params['QueueUrl'] = $this->config['endpoint'];
        }
        
        try {
            $this->client->deleteMessageBatchAsync($params);
        } catch (\Exception $ex) {
            Yii::error('SQSHandler deleteMessageBatchAsync Exception: Code: '.$ex->getCode().' ; Message: '.$ex->getMessage().' ; Trace: '.$ex->getTraceAsString());
            return false;
        }
        return true;
    }
}
