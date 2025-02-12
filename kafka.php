<?php

require 'vendor/autoload.php';

use RdKafka\Producer;
use RdKafka\Consumer;

// Produtor
$conf = new RdKafka\Conf();
$producer = new Producer($conf);
$producer->addBrokers('localhost:9092');

$topic = $producer->newTopic('mms.order');
$evento = [
    'evento' => 'mms.order.created',
    'id' => 123,
    'valor' => 99.90
];
$topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($evento));
$producer->flush(10000);

// Consumidor
$conf = new RdKafka\Conf();
$conf->set('group.id', 'meu_grupo');
$conf->set('metadata.broker.list', 'localhost:9092');

$consumer = new Consumer($conf);
$consumer->subscribe(['mms.order']);

while (true) {
    $message = $consumer->consume(1000);
    if ($message !== NULL) {
        $evento = json_decode($message->payload, true);
        echo json_encode($evento) . "\n";
    }
}
