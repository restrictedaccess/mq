<div style="font-family:'Courier New', Courier, monospace;">
<strong>System Executed Scheduled Staff Contract Salary Updates.</strong>
<ul>
    <li>Scheduled Date : {$sched.scheduled_date}</li>
    <li>Subcon Id : {$sched.subcontractors_id}</li>
    <li>Client : {$client.fname} {$client.lname}</li>
    <li>Staff : {$staff.fname} {$staff.lname} - {$subcon.job_designation}</li>
    <li>Scheduled by : {$created_by.fname} {$created_by.lname}</li>
    <li>Date Created : {$sched.date_added}</li>
</ul>

This is system generated.

{ if $recipients }
<hr />
Recipients. For Debugging purpose.
<ol>
{foreach from=$recipients item=recipient name=recipient}
<li>{$recipient}</li>
{/foreach}
</ol>
{ /if }
</div>