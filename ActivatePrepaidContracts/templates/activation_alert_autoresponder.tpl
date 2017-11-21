<div style="font-family:'Courier New', Courier, monospace; font-size:12px;">
<div style="text-transform:uppercase;">{$error_msg}</div>


{if $temp_contract}
<ul style="list-style:none;">
{foreach from=$temp_contract key=k item=v}
   <li>{$k} => {$v}</li>
{/foreach}
</ul>

{else}
<p>Contract does not exist!</p>
{/if}

</div>