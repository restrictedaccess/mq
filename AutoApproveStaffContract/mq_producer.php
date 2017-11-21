<?php
//  publisher script for syncing prod remotestaff/adhoc
require('conf/zend_smarty_conf.php');
require_once('C:/xampp/htdocs/portal-local/lib/php-amqplib/amqp.inc');

if(!$argv[1]){
    die('subcontractors_invoice_setup_id is missing');
}

//echo $argv[1];
//exit;
$exchange = 'subcontracts';
$queue = 'approve';
$consumer_tag = 'consumer';


$conn = new AMQPConnection(HOST, PORT, USER, PASS, VHOST);
$ch = $conn->channel();
$ch->queue_declare($queue, false, true, false, false);
$ch->exchange_declare($exchange, 'direct', false, true, false);
$ch->queue_bind($queue, $exchange);

$msg_body = $argv[1];

$msg = new AMQPMessage($msg_body, array('content_type' => 'text/plain', 'delivery-mode' => 2));
$ch->basic_publish($msg, $exchange);
$ch->close();
$conn->close();
?>