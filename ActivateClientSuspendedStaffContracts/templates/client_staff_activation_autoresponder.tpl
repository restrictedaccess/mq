<div style='font-family: "lucida grande",tahoma,verdana,arial,sans-serif; font-size: 11px;'>
<div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div>

<p style="text-transform:capitalize;">Dear {$client.fname} {$client.lname} ,</p>
 
<p>Your following staffs contract has been <strong>ACTIVATED</strong> and set contract status to "ACTIVE".</p>

<ol>
{foreach from=$subcons item=s name=s}

<li>{$s.staff_fname} {$s.staff_lname}  {$s.job_designation}</li>

{/foreach}
</ol>

<p>They can now continue working with you.</p>




<P>admin@remotestaff.com.au</P>
</div>