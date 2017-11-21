<?php
function disabled_staff_contracts($leads_id){
	include('conf/zend_smarty_conf.php');
	$smarty = new Smarty();
    $AusTime = date("H:i:s"); 
    $AusDate = date("Y")."-".date("m")."-".date("d");
    $ATZ = $AusDate." ".$AusTime;
    $admin_id = 5;
	$admin_name = 'Remote Staff';
	$admin_email = 'noreply@remotestaff.com.au';
	//global $db;
	//global $transport;
    
	
	if($leads_id==""){
        die('leads id is missing');
    }
	
	//client info
	$sql= $db->select()
	    ->from('leads')
		->where('id =?', $leads_id);
	$lead=$db->fetchRow($sql);	
	
	//get the list of subcontractors before we suspend the contract
	$sql = $db->select()
	    ->from(array('s' => 'subcontractors'))
		->join(array('p' => 'personal'), 'p.userid = s.userid', Array('fname', 'lname', 'email'))
		->where('s.status =?', 'ACTIVE')
		->where('s.prepaid =?', 'yes')
		->order('p.fname ASC')
		->where('s.leads_id =?', $leads_id);
	//echo $sql;exit;	
	$subcontractors = $db->fetchAll($sql);
	if(count($subcontractors) > 0){
		//disable the staff contract one by one
		foreach($subcontractors as $s){
			
			//echo sprintf('#%s => %s %s %s', $s['id'], $s['fname'], $s['lname'], $s['email']);
			//echo "\n--------\n";
			//
			
			//update the status and set the status to suspended
			$data = array('status' => 'suspended');
			$where = "id = ".$s['id'];	
			$db->update('subcontractors', $data , $where);
			
			//HISTORY
			//INSERT NEW RECORD TO THE subcontractors_history
			$changes = "SYSTEM AUTOMATICALLY SUSPENDED STAFF CONTRACT";
			$admin_notes = "System suspended staff contract due to client load issue";
			$data = array (
				'subcontractors_id' => $s['id'], 
				'date_change' => $ATZ, 
				'changes' => $changes, 
				'change_by_id' => $admin_id ,
				'changes_status' => 'suspended',
				'note' => $admin_notes
			);
			$db->insert('subcontractors_history', $data);
			
			//send autoresponder to staff
			$smarty->assign('s', $s);
			$smarty->assign('lead', $lead);
			$body = $smarty->fetch('staff_autoresponder.tpl');
				
			$mail = new Zend_Mail('utf-8');
			$mail->setBodyHtml($body);
			$mail->setFrom($admin_email, $admin_name);
				
			//SEND MAIL to the STAFF
			if(! TEST){
				$mail->addTo($s['email'], sprintf('%s %s' , $s['fname'], $s['lname']));
				$mail->setSubject("STAFF ".sprintf('%s %s' , $s['fname'], $s['lname'])." CONTRACT HAS BEEN SUSPENDED");
			}else{
				$mail->addTo('devs@remotestaff.com.au', 'Remotestaff Developers');
				$mail->setSubject("TEST STAFF ".sprintf('%s %s' , $s['fname'], $s['lname'])." CONTRACT HAS BEEN SUSPENDED");
			}
			
			//$mail->send($transport);
			
		}
	
	
		//SEND MAIL TO CLIENT
		$smarty->assign('subcontractors', $subcontractors);
		$smarty->assign('lead', $lead);
		$body = $smarty->fetch('client_autoresponder.tpl');
				
		$mail = new Zend_Mail('utf-8');
		$mail->setBodyHtml($body);
		$mail->setFrom($admin_email, $admin_name);
				
		if(! TEST){
			$mail->addTo($lead['email'], sprintf('%s %s' , $lead['fname'], $lead['lname']));
			$mail->setSubject("CLIENT STAFFS SUSPENSION NOTICE FOR ".sprintf('%s %s' , $lead['fname'], $lead['lname']));
		}else{
			$mail->addTo('devs@remotestaff.com.au', 'Remotestaff Developers');
			$mail->setSubject("TEST CLIENT STAFFS SUSPENSION NOTICE ".sprintf('%s %s' , $lead['fname'], $lead['lname']));
		}
			
		//$mail->send($transport);
		
		
		//SEND MAIL TO ADMIN
		$body = $smarty->fetch('admin_autoresponder.tpl');
		$mail = new Zend_Mail('utf-8');
		$mail->setBodyHtml($body);
		$mail->setFrom($admin_email, $admin_name);
				
		
		if(! TEST){
			//$mail->addTo('admin@remotestaff.com.au', 'Admin');
			//$mail->addTo('accounts@remotestaff.com.au', 'Accounts');
			
			//include the csro assign to lead
			if($lead['csro_id'] !=""){
			    $sql = $db->select()
				    ->from('admin')
					->where('admin_id =?', $lead['csro_id']);
				$csro = $db->fetchRow($sql);
				$mail->addTo($csro['admin_email'], $csro['admin_fname']);
				$mail->setSubject("STAFFS SUSPENSION NOTICE FOR ".sprintf('%s %s' , $lead['fname'], $lead['lname']));
			    $mail->addBcc('devs@remotestaff.com.au');
				$mail->send($transport);
			}
		}else{
			$mail->addTo('devs@remotestaff.com.au', 'Remotestaff Developers');
			$mail->setSubject("TEST STAFFS SUSPENSION NOTICE ".sprintf('%s %s' , $lead['fname'], $lead['lname']));
			$mail->send($transport);
		}
			
		
	}
	
}
?>