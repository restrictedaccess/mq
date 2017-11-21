<?php
//update_staff_salary($_GET['subcon_id']);
update_staff_salary($argv[1]);
function update_staff_salary($subcon_id){
	include('conf/zend_smarty_conf.php');
	
	$smarty = new Smarty();
	
    $smarty->template_dir = TEMPLATE_DIR;
    $smarty->compile_dir = COMPILE_DIR;
	
    $AusTime = date("H:i:s"); 
    $AusDate = date("Y")."-".date("m")."-".date("d");
    $ATZ = $AusDate." ".$AusTime;
    $admin_id = 5;
	$admin_name = 'Remote Staff';
	$admin_email = 'noreply@remotestaff.com.au';
	
	$send_auto_responder = False;
    
	
	if($subcon_id==""){
        die('subcontractors.id is missing');
    }
	
	$sql = $db->select()
	    ->from('subcontractors')
		->where('id=?', $subcon_id);
	$subcon = $db->fetchRow($sql);
	
	$sql = "SELECT * FROM subcontractors_scheduled_subcon_rate s WHERE s.status='waiting' AND s.subcontractors_id=".$subcon_id." ORDER BY id DESC LIMIT 1;";
	$sched =$db->fetchRow($sql);
	//print_r($sched);exit;
	if($sched){
		$history_changes="";
		
		//Client details
		$sql =$db->select()
		    ->from('leads', Array('fname', 'lname', 'email', 'csro_id'))
			->where('id=?', $subcon['leads_id']);
		$client = $db->fetchRow($sql);
		
		//Staff
		$sql =$db->select()
		    ->from('personal', Array('fname', 'lname', 'email'))
			->where('userid=?', $subcon['userid']);
		$staff = $db->fetchRow($sql);
		
		//Client CSRO
		if($client['csro_id']){
			$sql = $db->select()
			    ->from('admin', Array('admin_fname', 'admin_lname', 'admin_email') )
				->where('admin_id =?', $client['csro_id']);
			$csro = $db->fetchRow($sql);	
		}
		
		
		//Scheduled by
		if($sched['added_by_type'] == 'client'){
			$sql = $db->select()
				->from('leads', Array('fname', 'lname', 'email') )
				->where('id =?', $sched['added_by_id']);
			$created_by = $db->fetchRow($sql);
			
		}else if($sched['added_by_type'] == 'staff'){
			$sql = $db->select()
				->from('personal', Array('fname', 'lname', 'email') )
				->where('userid =?', $sched['added_by_id']);
			$created_by = $db->fetchRow($sql);
			
		}else{
			$sql = $db->select()
				->from('admin', Array('fname' => 'admin_fname', 'lname' => 'admin_lname', 'email' => 'admin_email') )
				->where('admin_id =?', $sched['added_by_id']);
			//echo $sql;exit;	
			$created_by = $db->fetchRow($sql);
		}
		
		
        
		
		if($sched['work_status'] == 'Full-Time'){
			$php_hourly = (((($sched['rate'] * 12)/52)/5)/8);
		}else{
			$php_hourly = (((($sched['rate'] * 12)/52)/5)/4);
		}
		
		$php_hourly = number_format($php_hourly ,2 ,'.' ,',');
		
		
		
		
		//update subcontractors
		
		$data=array(
            'php_monthly' => $sched['rate'],
			'php_hourly' => $php_hourly,
			'work_status' =>  ucwords($sched['work_status']),
        );
		
		if($sched['work_status'] != $subcon['work_status']){
			//echo sprintf('From %s To %s<br>', $subcon['work_status'], ucwords($sched['work_status']));
			$work_days = explode(',', $subcon['work_days']);
			
			if($sched['work_status'] == 'Full-Time'){
				$time_in_str = date('Y-m-d H:i:s', strtotime($subcon['client_finish_work_hour']));
				$data['client_finish_work_hour'] = date("H:i:s", strtotime ("+5 hour", strtotime($time_in_str)));
				foreach($work_days as $day){
				    $time_in_str = date('Y-m-d H:i:s', strtotime($subcon[$day."_finish"]));
					$data[$day."_finish"] = date("H:i:s", strtotime ("+5 hour", strtotime($time_in_str)));
					$data[$day."_start_lunch"] = '11:00:00';
					$data[$day."_finish_lunch"] = '12:00:00';
					$data[$day."_number_hrs"] = '8.00';
					$data[$day."_lunch_number_hrs"] = '1.00';
					
			    }
			}
			
			if($sched['work_status'] == 'Part-Time'){
				$time_in_str = date('Y-m-d H:i:s', strtotime($subcon['client_finish_work_hour']));
				$data['client_finish_work_hour'] = date("H:i:s", strtotime ("-5 hour", strtotime($time_in_str)));
				foreach($work_days as $day){
				    $time_in_str = date('Y-m-d H:i:s', strtotime($subcon[$day."_finish"]));
					$data[$day."_finish"] = date("H:i:s", strtotime ("-5 hour", strtotime($time_in_str)));
					$data[$day."_start_lunch"] = NULL;
					$data[$day."_finish_lunch"] = NULL;
					$data[$day."_number_hrs"] = '4.00';
					$data[$day."_lunch_number_hrs"] = '0.00';
			    }
			}
			
			
		}
		//echo "<pre>";
		//print_r($data);
		//echo "</pre>";
		//exit;
		foreach(array_keys($data) as $array_key){
			if($subcon[$array_key] != $data[$array_key]){
			    $history_changes .= sprintf("%s from %s to %s <br>", getFullColumnName($array_key), $subcon[$array_key] , $data[$array_key]);
			}
		}
		//echo $history_changes;
		//exit;
		
		$where = "id = ".$subcon_id;
		$db->update('subcontractors', $data, $where);
		
		//update schedules
		$data = array('status' => 'executed');
        $where = "status='waiting' AND subcontractors_id = ".$subcon_id;	
        $db->update('subcontractors_scheduled_subcon_rate', $data , $where);
		
		//HISTORY
        //INSERT NEW RECORD TO THE subcontractors_history
        $changes = "SYSTEM EXECUTED SCHEDULED STAFF CONTRACT SALARY UPDATES.<br>";
        $changes .= "<b>Changes made : </b><br>".$history_changes;
        $data = array (
		    'subcontractors_id' => $subcon_id, 
		    'date_change' => $ATZ, 
		    'changes' => $changes, 
		    'change_by_id' => '5' ,
		    'changes_status' => 'approved',
		    'note' => 'Cron System Executed.'
	    );
        $db->insert('subcontractors_history', $data);
		
		
		//Save Staff Rate
		$data=array(
			'subcontractors_id' =>$sched['subcontractors_id'], 
			'start_date' => $sched['scheduled_date'], 
			'rate' => $sched['rate'], 
			'work_status' => ucwords($sched['work_status'])	
        );
		$db->insert('subcontractors_staff_rate', $data);
		
		//Send email
		//Email recipient
		$recipients=array('admin@remotestaff.com.au', $csro['admin_email'], $created_by['email']);
		
		$smarty->assign('sched', $sched);
		$smarty->assign('client', $client);
		$smarty->assign('staff', $staff);
		$smarty->assign('subcon', $subcon);
		$smarty->assign('created_by', $created_by);
		
		
	    //echo "<pre>";
		//print_r($created_by);
		//echo "</pre>";
		//exit;
		
		$mail = new Zend_Mail('utf-8');    		
        $mail->setFrom('noreply@remotestaff.com.au', 'No Reply');
		
		
		
	    if( TEST){
		    $mail->addTo('devs@remotestaff.com.au', 'DEVS');
			$smarty->assign('recipients', $recipients);
		    $subject = sprintf("TEST Executed Scheduled Staff Contract Updates of #[%s] %s %s.", $sched['subcontractors_id'], $staff['fname'], $staff['lname']);
	    }else{
           
			foreach($recipients as $recipient){
				$mail->addTo($recipient, $recipient);
			}
			$mail->addBcc('devs@remotestaff.com.au');
	        $subject = sprintf("Executed Scheduled Staff Contract Updates of #[%s] %s %s.", $sched['subcontractors_id'], $staff['fname'], $staff['lname']);		    
	    }
		
		
		
		$body = $smarty->fetch('scheduled_staff_contract_salary_updates.tpl');
		
		$mail->setBodyHtml($body);
		$mail->setSubject($subject);	
	    $mail->send($transport);
				
	}
	
	return $subcon_id;	
}

function getFullColumnName($field){
	$field_name = array("leads_id" , "agent_id" ,"userid" , "posting_id" , "client_price" , "payment_type", "working_hours" , "working_days" , "php_monthly" ,"php_hourly" , "work_status" , "work_days" , "starting_date", "lunch_hour", "current_rate", "total_charge_out_rate" , "client_timezone", "client_start_work_hour", "client_finish_work_hour" , "currency" , "with_tax", "with_bp_comm", "with_aff_comm", "mon_start" , "mon_finish", "mon_number_hrs","mon_start_lunch","mon_finish_lunch" , "mon_lunch_number_hrs" , "tue_start" , "tue_finish", "tue_number_hrs", "tue_start_lunch" ,"tue_finish_lunch", "tue_lunch_number_hrs", "wed_start" , "wed_finish","wed_number_hrs" , "wed_start_lunch" ,"wed_finish_lunch" , "wed_lunch_number_hrs" , "thu_start" , "thu_finish", "thu_number_hrs", "thu_start_lunch" ,"thu_finish_lunch" ,"thu_lunch_number_hrs","fri_start","fri_finish","fri_number_hrs","fri_start_lunch" ,"fri_finish_lunch" , "fri_lunch_number_hrs" , "sat_start" , "sat_finish", "sat_number_hrs", "sat_start_lunch" ,"sat_finish_lunch" , "sat_lunch_number_hrs", "sun_start" , "sun_finish", "sun_number_hrs", "sun_start_lunch" ,"sun_finish_lunch" , "sun_lunch_number_hrs", "job_designation" , "staff_currency_id" , "staff_working_timezone", "flexi" , "overtime" , "overtime_monthly_limit", "client_price_effective_date", "prepaid_start_date", "staff_other_client_email", "staff_other_client_email_password", "reason", "reason_type", "replacement_request", "status", "service_type", "prepaid");
	
	$field_description = array("LEADS ID" , "AGENTS ID" ,"STAFF ID" , "ADS ID" , "CLIENT QUOTED PRICE" , "PAYMENT TYPE OPTION", "TOTAL WORKING HOURS PER WEEK" , "TOTAL WORKING DAYS PER WEEK" , "STAFF MONTHLY SALARY" ,"STAFF HOURLY SALARY" , "STAFF WORKING STATUS" , "WORKING DAYS" , "STAFF STARTING DATE", "TOTAL LUNCH HOURS", "CURRENT RATE", "CLIENT TOTAL CHARGE OUT RATE" , "CLIENT PREFFERED TIMEZONE", "CLIENT PREFFERED WORK START TIME", "CLIENT PREFFERED WORK FINISH TIME" , "CURRENCY" , "TAX INCLUDED", "BUSINESS PARTNER COMMISSION", "AFFILIATE COMMISSION", "STAFF MONDAY START WORKING TIME" , "STAFF MONDAY FINISH WORKING TIME", "STAFF MONDAY TOTAL WORKING HOURS","STAFF MONDAY START LUNCH TIME","STAFF MONDAY FINISH LUNCH TIME" , "STAFF MONDAY TOTAL LUNCH HOURS" , "STAFF TUESDAY START WORKING TIME" , "STAFF TUESDAY FINISH WORKING TIME", "STAFF TUESDAY TOTAL WORKING HOURS","STAFF TUESDAY START LUNCH TIME","STAFF TUESDAY FINISH LUNCH TIME" , "STAFF TUESDAY TOTAL LUNCH HOURS", "STAFF WEDNESDAY START WORKING TIME" , "STAFF WEDNESDAY FINISH WORKING TIME", "STAFF WEDNESDAY TOTAL WORKING HOURS","STAFF WEDNESDAY START LUNCH TIME","STAFF WEDNESDAY FINISH LUNCH TIME" , "STAFF THURSDAY TOTAL LUNCH HOURS", "STAFF THURSDAY START WORKING TIME" , "STAFF THURSDAY FINISH WORKING TIME", "STAFF THURSDAY TOTAL WORKING HOURS","STAFF THURSDAY START LUNCH TIME","STAFF THURSDAY FINISH LUNCH TIME" , "STAFF FRIDAY TOTAL LUNCH HOURS", "STAFF FRIDAY START WORKING TIME" , "STAFF FRIDAY FINISH WORKING TIME", "STAFF FRIDAY TOTAL WORKING HOURS","STAFF FRIDAY START LUNCH TIME","STAFF FRIDAY FINISH LUNCH TIME" , "STAFF FRIDAY TOTAL LUNCH HOURS", "STAFF SATURDAY START WORKING TIME" , "STAFF SATURDAY FINISH WORKING TIME", "STAFF SATURDAY TOTAL WORKING HOURS","STAFF SATURDAY START LUNCH TIME","STAFF SATURDAY FINISH LUNCH TIME" , "STAFF SATURDAY TOTAL LUNCH HOURS", "STAFF SUNDAY START WORKING TIME" , "STAFF SUNDAY FINISH WORKING TIME", "STAFF SUNDAY TOTAL WORKING HOURS","STAFF SUNDAY START LUNCH TIME","STAFF SUNDAY FINISH LUNCH TIME" , "STAFF SUNDAY TOTAL LUNCH HOURS" , "JOB DESIGNATION" , "STAFF SALARY CURRENCY" , "STAFF WORKING TIMEZONE" , "FLEXI SCHEDULE",  "APPROVE ALL OVER TIMES" , "OVER TIME MONTHLY LIMIT", "EFFECTIVE DATE OF THE NEW CLIENT PRICE", "PREPAID STAFF START DATE", "STAFF OTHER CLIENT EMAIL", "STAFF OTHER CLIENT EMAIL PASSWORD", "REASON", "REASON TYPE", "REPLACEMENT REQUEST", "CONTRACT STATUS", "SERVICE TYPE", "PREPAID");
	
	for($i=0;$i<count($field_name);$i++){
		if($field == $field_name[$i]){
			return $field_description[$i];
			break;
		}
	}
	
}
?>