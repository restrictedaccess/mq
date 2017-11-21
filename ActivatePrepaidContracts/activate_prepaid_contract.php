<?php
//activate_prepaid_contract($_GET['id']);
global $db;
require_once('CouchDBMailbox.php');
	
function activate_prepaid_contract($id){
	echo "Received ".$id."\n";
	global $db;
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
    
	
	if($id==""){
        //die('subcontractors_temp.id is missing');
        return 'subcontractors_temp.id is missing';
    }
	
	$sql = $db->select()
	    ->from('subcontractors_temp')
		->where('temp_status =?', 'new')
		->where('id=?', $id);
	$temp_contract = $db->fetchRow($sql);
	
	//print_r($temp_contract);
	
	if($temp_contract['id']) {
	
	
		if($temp_contract['subcontractors_id']){
		
			//means this contract is converted
			//we should parse the original contract details in subcontractors table and clone it then insert new record
			$sql = "SELECT * FROM subcontractors WHERE id = ". $temp_contract['subcontractors_id'];
			$original_contract = $db->fetchRow($sql);
			$original_subcon_id = $original_contract['id'];
			//echo $original_subcon_id;exit;
			////cloned the exsting contract history
			$sql = "SELECT * FROM subcontractors_history s WHERE subcontractors_id =".$temp_contract['subcontractors_id'] ;
			$original_contract_histories = $db->fetchAll($sql);
			
			//setup the clone and involved value
			unset($original_contract['id']);
			$original_contract['prepaid'] = 'yes';
					
			//cloned and insert new record
			$db->insert('subcontractors', $original_contract);
			$new_subcontractors_id = $db->lastInsertId();
			
			
			//Save client rate
			$data = array(
	            'subcontractors_id' => $new_subcontractors_id, 
		        'start_date' => $original_contract['starting_date'], 
		        'rate' => $original_contract['client_price'],
				'work_status' => $original_contract['work_status']
	        );
		    $db->insert('subcontractors_client_rate', $data);
			
			
			//Save Staff Rate
		    $data=array(
			    'subcontractors_id' =>$new_subcontractors_id, 
			    'start_date' => $original_contract['starting_date'], 
			    'rate' => $original_contract['php_monthly'], 
			    'work_status' => $original_contract['work_status']	
            );
		    $db->insert('subcontractors_staff_rate', $data);
			
			
			//update the existing record
			$changes = sprintf('cloned , converted to prepaid, and deleted. new subcontractors.id => %s', $new_subcontractors_id);
			$data = array(
				'status' => 'deleted',
				'reason' => $changes
			);
			$db->update('subcontractors', $data, 'id='.$original_subcon_id);
	
			$data = array (
				'subcontractors_id' => $original_subcon_id, 
				'date_change' => $ATZ, 
				'changes' => $changes,
				'change_by_type' => 'admin',
				'change_by_id' => $admin_id ,
				'changes_status' => 'terminated',
				'note' => 'deleted and cloned due to prepaid conversion'
			);
			$db->insert('subcontractors_history', $data);
			
			
			//cloned the histories
			foreach($original_contract_histories as $h){
				$data = array (
					'subcontractors_id' => $new_subcontractors_id, 
					'date_change' => $h['date_change'], 
					'changes' => $h['changes'], 
                    'change_by_type' => $h['change_by_type'],					
					'change_by_id' => $h['change_by_id'] ,
					'changes_status' => $h['changes_status'],
					'note' => $h['note']
				);
				$db->insert('subcontractors_history', $data);
			}
			
			
			$data = array (
				'subcontractors_id' => $new_subcontractors_id, 
				'date_change' => $ATZ, 
				'changes' => 'System automatic approved scheduled prepaid contract',
				'change_by_type' => 'admin',
				'change_by_id' => $admin_id ,
				'changes_status' => 'approved',
				'note' => 'System Executed'
			);
			$db->insert('subcontractors_history', $data);
			
			
			$send_auto_responder = False;
			
			//echo "Clone Record<pre>";
			//print_r($original_contract);
			//echo "</pre>";
			
		}else{
			//newly created contract
			//get the details from subcontractors_temp table
			$userid = $temp_contract['userid'];
			$temp_staff_email = $temp_contract['staff_email'];
			
			//$sql="SELECT * FROM personal WHERE userid = $userid;";
			//$personal = $db->fetchRow($sql);
						
			
			$sql = "SELECT * FROM subcontractors_temp_history s WHERE subcontractors_id =".$temp_contract['id'] ;
			$temp_contract_histories = $db->fetchAll($sql);
			
			
			unset($temp_contract['id']);
			unset($temp_contract['subcontractors_id']);
			unset($temp_contract['temp_status']);
			unset($temp_contract['modified_fields']);
			
			//$initial_email_password = $temp_contract['initial_email_password'];
			//unset($temp_contract['skype_id']);
			//unset($temp_contract['initial_email_password']);
			//unset($temp_contract['initial_skype_password']);
			
			$initial_email_password = $temp_contract['initial_email_password'];
			$initial_skype_password = $temp_contract['initial_skype_password'];
			
			$temp_contract['initial_skype_password'] = $initial_skype_password;
			$temp_contract['initial_skype_password'] = $initial_skype_password;
			
			$temp_contract['contract_updated'] = 'y';
			$temp_contract['status'] = 'ACTIVE';
			$temp_contract['client_price_effective_date'] = $temp_contract['starting_date'];
			//insert new record in the subcontractors table
			$db->insert('subcontractors', $temp_contract);
			$new_subcontractors_id = $db->lastInsertId();
			
			
			//Save client rate
			$data = array(
	            'subcontractors_id' => $new_subcontractors_id, 
		        'start_date' => $temp_contract['starting_date'], 
		        'rate' => $temp_contract['client_price'],
				'work_status' => $temp_contract['work_status']
	        );
		    $db->insert('subcontractors_client_rate', $data);
			
			
			//Save Staff Rate
		    $data=array(
			    'subcontractors_id' =>$new_subcontractors_id, 
			    'start_date' => $temp_contract['starting_date'], 
			    'rate' => $temp_contract['php_monthly'], 
			    'work_status' => $temp_contract['work_status']	
            );
		    $db->insert('subcontractors_staff_rate', $data);
			
			//$where = "userid = ".$userid;
            //$data = array('registered_email' => $personal['email'] , 'email' => $temp_staff_email, 'initial_email_password' => $initial_email_password);
            //$db->update('personal', $data , $where);

			
			//echo "New Record<pre>";
			//print_r($temp_contract);
			//echo "</pre>";
			//exit;
			
			//cloned the histories
			foreach($temp_contract_histories as $h){
				$data = array (
					'subcontractors_id' => $new_subcontractors_id, 
					'date_change' => $h['date_change'], 
					'changes' => $h['changes'],
					'change_by_type' => $h['change_by_type'],
					'change_by_id' => $h['change_by_id'] ,
					'changes_status' => $h['changes_status'],
					'note' => $h['note']
				);
				$db->insert('subcontractors_history', $data);
			}
			
			
			$data = array (
				'subcontractors_id' => $new_subcontractors_id, 
				'date_change' => $ATZ, 
				'changes' => 'System automatic approved contract', 
				'change_by_type' => 'admin',
				'change_by_id' => $admin_id ,
				'changes_status' => 'approved',
				'note' => 'System Executed'
			);
			$db->insert('subcontractors_history', $data);
			
			
			//Check if temp_id is existing in client_subcontractors_lookup table
			$sql = $db->select()
			    ->from('client_subcontractors_lookup', 'id')
				->where('temp_id=?', $id);
			$client_subcontractors_lookup_id = $db->fetchOne($sql);
			$created_by_client = false;
			if($client_subcontractors_lookup_id){
				$created_by_client = true;
				$data=array('subcon_id' => $new_subcontractors_id);
				$db->update('client_subcontractors_lookup', $data, 'id='.$client_subcontractors_lookup_id);
			}
			
			
			$send_auto_responder = True;
		}
		
		$data = array('subcontractors_id' => $new_subcontractors_id ,'temp_status' => 'deleted');
		$db->update('subcontractors_temp', $data, 'id='.$id);
		
		
		if($send_auto_responder == True){
			$sql = $db->select()
				 ->from(array('s' => 'subcontractors'))
				 ->join(array('p' => 'personal'), 'p.userid = s.userid', Array('fname', 'lname', 'email'))
				 ->join(array('l' => 'leads'), 'l.id = s.leads_id', Array('client_fname' => 'fname', 'client_lname' => 'lname', 'client_email' => 'email', 'company_name', 'registered_domain', 'csro_id'))
				 ->where('s.status =?', 'ACTIVE')
				 ->where('s.id =?', $new_subcontractors_id);
			$subcon = $db->fetchRow($sql);
			//echo $sql."<br>";
			//echo "<pre>";
			//print_r($subcon);
			//echo "</pre>";
			
			$staff_name = unicode_str($subcon['fname'])." ".unicode_str($subcon['lname']);
			$staff_email = $subcon['staff_email'];
			$job_designation = $subcon['job_designation'];
			$work_status = $subcon['work_status'];
			$client_name = unicode_str($subcon['client_fname'])." ".unicode_str($subcon['client_lname']);
			$client_email = $subcon['client_email'];
			$admin_name = 'Remote Staff';
			$admin_email = 'noreply@remotestaff.com.au';
			$registered_email = $subcon['email'];
			$skype = $subcon['skype_id'];
			$staff_email_password = $initial_email_password;
			$skype_password  = $initial_skype_password;
			$date=date('l jS \of F Y h:i:s A');
			$main_pass="<i>(Your specified password on your jobseeker account)</i>";
			$registered_domain = $subcon['registered_domain'];
			$starting_date = $subcon['starting_date'];
			$company_name = $subcon['company_name'];
			
			
			//$staff_name = utf8_encode($staff_name);
			//$staff_name = utf8_decode($staff_name);
			
			if($subcon['csro_id'] != ""){
				$sql=$db->select()
				    ->from('admin')
					->where('admin_id=?', $subcon['csro_id']);
				$csro = $db->fetchRow($sql);	
			}
			
			if(TEST){
				$site = 'test.remotestaff.com.au';
			}else{
				$site = 'remotestaff.com.au';
			}
			
			if($csro){
			    $admin_email = $csro['admin_email'];
				$admin_name = sprintf('%s %s', $csro['admin_fname'], $csro['admin_lname']);
			}
			
			//send email to admin
			$details =  "<h3>SYSTEM AUTOMATIC APPROVED STAFF CONTRACT </h3>
					<p>Staff : ".$staff_name."</p>
					<p>Job Designation : ".$job_designation."</p>
					<p>Working : ".$work_status."</p>
					<p>Client : ".$client_name."</p>
					<p>Approved by : Remote Staff System</p>";
					
			$subject ="STAFF ".$staff_name." CONTRACT HAS BEEN APPROVED";
			$to_array=array('devs@remotestaff.com.au');
			
			
			//SaveToCouchDBMailbox(NULL, NULL, NULL, $admin_email, $details, $subject, NULL, $to_array, $sender=NULL, $reply_to=NULL);
			
			if($created_by_client){
				//send email to client
				$client_start_work_hour = formatTime($subcon['client_start_work_hour']);
				$client_finish_work_hour  = formatTime($subcon['client_finish_work_hour']);
				
				$start_date_of_leave = explode('-',$subcon['starting_date']);
				$date = new DateTime();
				$date->setDate($start_date_of_leave[0], $start_date_of_leave[1], $start_date_of_leave[2]);
				
				
				$date->modify("+1 month");
				$first_month = $date->format("Y-m-d");
				$end_of_first_month = $date->format('Y-m-t');
				$date->modify("+1 month");
				$third_month = $date->format("Y-m-01");
				$end_of_third_month = $date->format('Y-m-t');
				
				$department_email = 'clientsupport@remotestaff.com.au';
				
				$smarty->assign('department_email', $department_email);
				$smarty->assign('first_month',$first_month);
				$smarty->assign('end_of_first_month',$end_of_first_month);
				$smarty->assign('third_month',$third_month);
				$smarty->assign('end_of_third_month',$end_of_third_month);
				
				$smarty->assign('job_designation',$job_designation);
				$smarty->assign('staff_name',$staff_name);
				$smarty->assign('staff_email',$staff_email);
				$smarty->assign('starting_date',$starting_date);
				$smarty->assign('client_start_work_hour',$client_start_work_hour);
				$smarty->assign('client_finish_work_hour',$client_finish_work_hour);
				$smarty->assign('skype',$skype);
				$smarty->assign('client_name', $client_name);
				$smarty->assign('client_email',$client_email);
				$body = $smarty->fetch('client_created_client_autoresponder.tpl');
				
				$subject = "Confirming Staff ".ucwords(strtolower($staff_name))." First Day, Work Contact Details, Tools And Others.";
				$from = 'clientsupport@remotestaff.com.au';
				$to_array=array('staff.contract.approvals@remotestaff.com.au', $client_email);				
				$bcc_array=array('devs@remotestaff.com.au');
				$html=$body;
				SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
				
				
				//autoresponder to staff
				$smarty->assign('date',$date);
				$smarty->assign('staff_name', $staff_name);
				$smarty->assign('staff_email', $staff_email);
				$smarty->assign('registered_email', $registered_email);
				$smarty->assign('main_pass', $main_pass);
				$smarty->assign('site', $site);
				$smarty->assign('skype', $skype);
				$smarty->assign('skype_password', $skype_password);
				$smarty->assign('staff_email_password', $staff_email_password);
				$smarty->assign('client_name', $client_name);
				$smarty->assign('company_name', $company_name);
				$body = $smarty->fetch('client_created_staff_autoresponder.tpl');
				
				$subject = "Welcome To Remote Staff ".ucwords(strtolower($staff_name));
				$to_array=array($staff_email);
				if($registered_email!=""){
					$to_array[]=$registered_email;
				}

				$bcc_array=array('devs@remotestaff.com.au');
				$cc_array=array('staff.contract.approvals@remotestaff.com.au');
				$from= 'staffsupport@remotestaff.com.au';
				$html=$body;
				SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
			}else{
				//autoresponder to staff
				$smarty->assign('date',$date);
				$smarty->assign('staff_name', $staff_name);
				$smarty->assign('staff_email', $staff_email);
				$smarty->assign('registered_email', $registered_email);
				$smarty->assign('main_pass', $main_pass);
				$smarty->assign('site', $site);
				$smarty->assign('skype', $skype);
				$smarty->assign('skype_password', $skype_password);
				$smarty->assign('staff_email_password', $staff_email_password);
				$smarty->assign('client_name', $client_name);
				$smarty->assign('company_name', $company_name);
				$smarty->assign('admin_name', $admin_name);
				$smarty->assign('admin_email', $admin_email);
				$body = $smarty->fetch('staff_autoresponder.tpl');
				
				$subject = "Welcome To Remote Staff ".ucwords(strtolower($staff_name));
				$to_array=array('staff.contract.approvals@remotestaff.com.au', $staff_email);
				if($registered_email!=""){
					$to_array[]=$registered_email;
				}
				if($csro){
			    	$to_array[]=$csro['admin_email'];
				}
				$bcc_array=array('devs@remotestaff.com.au');
				$cc_array=NULL;
				$from= $admin_email;
				$html=$body;
				SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
				
				
				//SEND MAIL to the CLIENT
				$client_start_work_hour = formatTime($subcon['client_start_work_hour']);
				$client_finish_work_hour  = formatTime($subcon['client_finish_work_hour']);
				
				$start_date_of_leave = explode('-',$subcon['starting_date']);
				$date = new DateTime();
				$date->setDate($start_date_of_leave[0], $start_date_of_leave[1], $start_date_of_leave[2]);
				
				
				$date->modify("+1 month");
				$first_month = $date->format("Y-m-d");
				$end_of_first_month = $date->format('Y-m-t');
				$date->modify("+1 month");
				$third_month = $date->format("Y-m-01");
				$end_of_third_month = $date->format('Y-m-t');
				
				$department_email = 'clientsupport@remotestaff.com.au';
				
				
				
				$smarty->assign('department_email', $department_email);
				$smarty->assign('first_month',$first_month);
				$smarty->assign('end_of_first_month',$end_of_first_month);
				$smarty->assign('third_month',$third_month);
				$smarty->assign('end_of_third_month',$end_of_third_month);
				
				$smarty->assign('job_designation',$job_designation);
				$smarty->assign('staff_name',$staff_name);
				$smarty->assign('staff_email',$staff_email);
				$smarty->assign('starting_date',$starting_date);
				$smarty->assign('client_start_work_hour',$client_start_work_hour);
				$smarty->assign('client_finish_work_hour',$client_finish_work_hour);
				$smarty->assign('skype',$skype);
				$smarty->assign('client_name', $client_name);
				$smarty->assign('client_email',$client_email);
				$smarty->assign('csro',$csro);
				$body = $smarty->fetch('client_autoresponder.tpl');
				
				$subject = "Confirming Staff ".ucwords(strtolower($staff_name))." First Day, Work Contact Details, Tools And Others.";
				if($csro){
					$from = $csro['admin_email'];
				}else{
					$from = 'CSRO@remotestaff.com.au';
				}
			
				$to_array=array('staff.contract.approvals@remotestaff.com.au', $client_email);
				
				if($csro){
			    	$cc_array[]=$csro['admin_email'];
				}
				$bcc_array=array('devs@remotestaff.com.au');
				$html=$body;
				SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
			}
			
			//Accounts Autoreponder content
			$body =  "<h3>NEW STAFF CONTRACT </h3>
					  <p>Dear Accounts,</p>
					  <p>There is a new contract set between ".$staff_name." and ".$client_name." .  Start day will be on ".$subcon['starting_date']." Work Schedule is ".$subcon['client_timezone']." ".$client_start_work_hour." to ".$client_finish_work_hour." time. </p>";			
			$body .="<p>Please issue first month invoice and collect payments before the first day of this contract. If not paid, contact CSRO to adjust the first day</p>";
			$body .="<p><strong>FYI</strong><br />Service Type : ".$subcon['service_type']."</p>";
			
			$subject = "Accounts: New Staff Contract for ".ucwords(strtolower($staff_name));
			$from = $admin_email;
		
			$to_array=array('accounts@remotestaff.com.au');
			$cc_array=NULL;
			$bcc_array=array('devs@remotestaff.com.au');
			$html=$body;
			SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
			
			//send email to recruiter staff
			$sql = "SELECT r.id, r.userid, a.admin_fname, a.admin_email FROM recruiter_staff r JOIN admin a ON a.admin_id = r.admin_id WHERE r.userid=".$userid;
			$recruiter_staff = $db->fetchRow($sql);
			if($recruiter_staff['id']){
				$smarty->assign('recruiter_staff',$recruiter_staff);
				$smarty->assign('client_name', $client_name);
				$smarty->assign('staff_name', $staff_name);
				$smarty->assign('job_designation', $job_designation);
				$smarty->assign('starting_date', $starting_date);
				$body = $smarty->fetch('recruiter_staff_autoresponder.tpl');
				
				$subject = sprintf("Your candidate %s has been hired.", $staff_name);
				$from = 'noreply@remotestaff.com.au';
			
				$to_array=array($recruiter_staff['admin_email']);
				$cc_array=NULL;
				$bcc_array=array('devs@remotestaff.com.au');
				$html=$body;
				SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
			}
			
			
			//motivational autoresponder
            if($subcon['leads_id'] != 11){
                $smarty->assign('client_name', $client_name);
                $smarty->assign('staff_name', $staff_name);
                $smarty->assign('job_designation', $job_designation);
                $smarty->assign('starting_date', $starting_date);
                $body = $smarty->fetch('motivational_autoresponder.tpl');
                
                $subject = "Congratulations!!!! High Fives all round!";
                $from = 'noreply@remotestaff.com.au';
            
                $to_array=array();
                $to_array[] = 'lance@remotestaff.com.au';
                if($recruiter_staff['id']){
                    $to_array[] = $recruiter_staff['admin_email'];
                }
                
                if($csro){
                    $to_array[] = $csro['admin_email'];
                }
                
                $cc_array=NULL;
                $bcc_array=array('devs@remotestaff.com.au');
                $html=$body;
                SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
            }
			//exit;
		}	
	}else{
	    
		$sql = $db->select()
	        ->from('subcontractors_temp')
		    ->where('id=?', $id);
	    $temp_contract = $db->fetchRow($sql);
		
		
		
		if($temp_contract['id']){
		    $error_msg =sprintf('subcontractors_temp.id [ %s ] cannot be executed.', $id);
		}else{
		    $error_msg =sprintf('[ %s ] id is not existing in subcontractors_temp table.', $id);
		}
		
		$smarty->assign('error_msg', $error_msg);
		$smarty->assign('temp_contract', $temp_contract);
		$body = $smarty->fetch('activation_alert_autoresponder.tpl');			
		$subject = "STAFF CONTRACT ACTIVATION ALERT";
		$from = 'noreply@remotestaff.com.au';
	
		$to_array=array('devs@remotestaff.com.au');
		$cc_array=NULL;
		$bcc_array=NULL;
		$html=$body;
		SaveToCouchDBMailbox($attachments_array=NULL, $bcc_array, $cc_array, $from, $html, $subject, $text=NULL, $to_array, $sender=NULL, $reply_to=NULL);
		
		return $id;
	}
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

function unicode_str($string){
	return html_entity_decode(htmlentities($string));
}
?>