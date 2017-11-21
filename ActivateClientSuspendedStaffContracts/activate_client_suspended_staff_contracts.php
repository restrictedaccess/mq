<?php
function activate_client_suspended_staff_contracts($leads_id){
	global $db;
	global $transport;
	echo "Received {$leads_id}\n";
	
	$smarty = new Smarty();
	
    $smarty->template_dir = TEMPLATE_DIR;
    $smarty->compile_dir = COMPILE_DIR;
	
    $AusTime = date("H:i:s"); 
    $AusDate = date("Y")."-".date("m")."-".date("d");
    $ATZ = $AusDate." ".$AusTime;
    $admin_id = 5;
	$admin_name = 'Remote Staff MQ';
	$admin_email = 'noreply@remotestaff.com.au';
    
	
	if($leads_id==""){
        //die('leads_id is missing');
		return 'leads_id is missing';
    }
	
	$sql = $db->select()
	    ->from('leads')
		->where('id=?', $leads_id);
	$client = $db->fetchRow($sql);	
	
	$sql= $db->select()
        ->from(array('s' => 'subcontractors'))
	    ->join(array('p' => 'personal'), 'p.userid = s.userid', Array('staff_email' => 'email', 'staff_fname' => 'fname', 'staff_lname' => 'lname'))
		->where("s.status = 'suspended'")
	    ->where('s.leads_id =?', $leads_id)
		->order('p.fname ASC');
    $subcons =$db->fetchAll($sql);
	
	if(count($subcons) == 0){
	    echo "ALERT : No Suspended Staff contracts detected with leads.id => ". $leads_id;
		/*
		//SEND MESSAGE TO DEVELOPERS
		$smarty->assign('client',$client);
        $body = $smarty->fetch('staff_activation_autoresponder_alert.tpl');
		
		$mail = new Zend_Mail('utf-8');
	    $mail->setBodyHtml($body);
	    $mail->setFrom($admin_email, $admin_name);
	    if(! TEST){
		    $mail->setSubject("MQ PROCESS ACTIVATE CLIENT SUSPENDED STAFF CONTRACTS ALERT");
	    }else{
		    $mail->setSubject("TEST MQ PROCESS ACTIVATE CLIENT SUSPENDED STAFF CONTRACTS ALERT");
	    }
		$mail->addTo('devs@remotestaff.com.au', "Remotestaff Developers");
	    $mail->send($transport);
		*/
		//exit;
	}else{
	
		foreach($subcons as $subcon){
			 //echo sprintf('[%s] => #%s %s %s %s<br>', $subcon['id'], $subcon['userid'], $subcon['staff_fname'], $subcon['staff_lname'], $subcon['staff_email']);
			 $subcontractors_id = $subcon['id'];
			 //activate the staff contract
			 $data = array('status' => 'ACTIVE');
			 $where = "id = ".$subcontractors_id;
			 $db->update('subcontractors', $data , $where);
			 
			 
			 //HISTORY
			//INSERT NEW RECORD TO THE subcontractors_history
			$changes = "Activated suspended staff contract. Status set to ACTIVE.";
			$admin_notes = "Automactically activated.";
			$data = array (
					'subcontractors_id' => $subcontractors_id, 
					'date_change' => $ATZ, 
					'changes' => $changes, 
					'change_by_id' => $admin_id ,
					'changes_status' => 'updated',
					'note' => $admin_notes
					);
			$db->insert('subcontractors_history', $data);
			
			//SEND EMAIL TO STAFF
			if(! TEST){
				$subject = "STAFF ".sprintf('%s %s', $subcon['staff_fname'], $subcon['staff_lname'])." CONTRACT ACTIVATED";
			}else{
				$subject = "TEST STAFF ".sprintf('%s %s', $subcon['staff_fname'], $subcon['staff_lname'])." CONTRACT ACTIVATED";
			}
			
			$smarty->assign('subcon',$subcon);
			$smarty->assign('client',$client);
			
			$body = $smarty->fetch('staff_activation_autoresponder.tpl');
	
			$mail = new Zend_Mail('utf-8');
			$mail->setBodyHtml($body);
			$mail->setFrom($admin_email, $admin_name);
			
			if(! TEST){
				//admin email
				$mail->addTo($subcon['staff_email'], sprintf('%s %s', $subcon['staff_fname'], $subcon['staff_lname']));
			}else{
				$mail->addTo('devs@remotestaff.com.au', "Remotestaff Developers");
			}
			
			$mail->setSubject($subject);
			//$mail->send($transport);
			
	
		}
		
		
		//AUTORESPONDERS
		//send email to client
		$smarty->assign('subcons',$subcons);
		$smarty->assign('client',$client);
		$body2 = $smarty->fetch('client_staff_activation_autoresponder.tpl');
		$body3 = $smarty->fetch('admin_client_staff_activation_autoresponder.tpl');
		
		$mail = new Zend_Mail('utf-8');
		$mail->setBodyHtml($body2);
		$mail->setFrom($admin_email, $admin_name);
		
		if(! TEST){
			//admin email
			$mail->addTo($client['email'], sprintf('%s %s', $client['fname'], $client['lname']) );
			//$mail->addBcc('devs@remotestaff.com.au');
			$mail->setSubject("STAFFS CONTRACT ACTIVATED");
		}else{
			$mail->addTo('devs@remotestaff.com.au', "Remotestaff Developers");
			$mail->setSubject("TEST STAFFS CONTRACT ACTIVATED");
		}
		
		
		//$mail->send($transport);
	
	
		//send email to admin
		$mail = new Zend_Mail('utf-8');
		$mail->setBodyHtml($body3);
		$mail->setFrom($admin_email, $admin_name);
		if(! TEST){
			//admin email
			//$mail->addTo('admin@remotestaff.com.au', 'Admin');
			//$mail->addTo('accounts@remotestaff.com.au', 'Accounts');
			
			//include the csro assign to lead
		    if($client['csro_id'] !=""){
		        $sql = $db->select()
			        ->from('admin')
				    ->where('admin_id =?', $client['csro_id']);
			    $csro = $db->fetchRow($sql);
			    $mail->addTo($csro['admin_email'], $csro['admin_fname']);	
				$mail->addBcc('devs@remotestaff.com.au');
			    $mail->setSubject("STAFFS CONTRACT ACTIVATED ALERT");
				$mail->send($transport);
		    }
		    
		}else{
			$mail->addTo('devs@remotestaff.com.au', "Remotestaff Developers");
			$mail->setSubject("TEST STAFFS CONTRACT ACTIVATED ALERT");
			$mail->send($transport);
		}
		
		
	}
	//exit;
	
	
}

function formatTime($time ){
	if($time!=""){
		if(strlen($time) > 2){
			$time_str = $time;
		}else{
			$time_str = $time.":00";
		}
		$time = new DateTime($time_str);
		return $time->format('h:i a');
	}
}
?>