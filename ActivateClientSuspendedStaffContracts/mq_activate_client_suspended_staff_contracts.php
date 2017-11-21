<?php
//  publisher script for syncing prod remotestaff/adhoc
//  2012-01-18  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au
//  -   blanked exchanged, updated queue and moved amqplib

require('conf/zend_smarty_conf.php');
require('conf/conf.php');
include 'activate_client_suspended_staff_contracts.php';

$exchange = '/';   //apparently empty string is not allowed and the default is '/', exchange must be a valid one although not really used in these scenario
$queue = 'activate_client_suspended_staff_contracts';
$consumer_tag = 'consumer';

$conn = new AMQPConnection(HOST, PORT, USER, PASS, VHOST);
$ch = $conn->channel();
$ch->queue_declare($queue, false, true, false, false);
$ch->exchange_declare($exchange, 'direct', false, true, false);
$ch->queue_bind($queue, $exchange);

function process_message($msg) {

    echo "\n--------\n";
    echo $msg->body;
    echo "\n--------\n";

    $msg->delivery_info['channel']->
        basic_ack($msg->delivery_info['delivery_tag']);
    
	//function to activate all suspended staff contracts of a client
	//leads_id 
	activate_client_suspended_staff_contracts($msg->body);
	 
    // Send a message with the string "quit" to cancel the consumer.
    if ($msg->body === 'quit') {
        $msg->delivery_info['channel']->
            basic_cancel($msg->delivery_info['consumer_tag']);
    }
}


$ch->basic_consume($queue, $consumer_tag, false, false, false, false, 'process_message');

function shutdown($ch, $conn){
    $ch->close();
    $conn->close();
}
register_shutdown_function('shutdown', $ch, $conn);

// Loop as long as the channel has callbacks registered
while(count($ch->callbacks)) {
    $ch->wait();
}
?>