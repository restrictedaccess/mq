<div style='font-family: "lucida grande",tahoma,verdana,arial,sans-serif; font-size: 11px;'>
<div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div>

<p style="text-transform:capitalize;">Dear {$lead.fname} {$lead.lname},</p>
 
<p>Your staff/s contract to you is/are temporarily suspended.</p>
<p>They are no longer be able to login to their RSSC and in to the Remotestaff System.</p>
<p>We will activate these staff/s as soon as payment is made.</p>

<p>Following Staffs:</p>
<ol>
{foreach from=$subcontractors item=s name=s}
<li style="text-transform:capitalize; padding:5px;">
{$s.fname} {$s.lname} <span style="margin-left:20px;">[<em> {$s.job_designation} </em>]</span>
</li>
{/foreach}
</ol>
<P>admin@remotestaff.com.au</P>
</div>