<div style='font-family: "lucida grande",tahoma,verdana,arial,sans-serif; font-size: 11px;'>
<div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div>

<p style="text-transform:capitalize;">Dear Admin ,</p>
 
<p>Client #{$client.id} {$client.fname} {$client.lname} following staffs contract has been <strong>ACTIVATED</strong> and set contract status to "ACTIVE".</p>

<ol>
{foreach from=$subcons item=s name=s}

<li style="text-transform:capitalize;">#{$s.userid} {$s.staff_fname} {$s.staff_lname} =>  {$s.job_designation}</li>

{/foreach}
</ol>

<p>&nbsp;</p>
<P style="color:#999999; font-family:'Courier New', Courier, monospace;">Message from : <br>MQ Process [Remote Staff]</P>
</div>