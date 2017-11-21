<div style='font-family: "lucida grande",tahoma,verdana,arial,sans-serif; font-size: 11px;'>
<div style="margin-bottom:20px;"><img src="https://remotestaff.com.au/images/remote-staff-logo2.jpg"></div>

<p><span style="text-transform:capitalize;">Client #{$lead.id } {$lead.fname} {$lead.lname}</span> {$lead.email} subcontractors
has been temporarily suspended due to client's depleted load balance.
</p>
 

<p>Following Staff/s:</p>
<ol>
{foreach from=$subcontractors item=s name=s}
<li style="text-transform:capitalize; padding:5px;">
{$s.fname} {$s.lname} <span style="margin-left:20px;">[<em> {$s.job_designation} </em>]</span>
</li>
{/foreach}
</ol>


<p style="color:#999999;">Remotestaff System
<br />
System Generated.
</p>

</div>