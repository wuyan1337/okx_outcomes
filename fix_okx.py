#!/usr/bin/env python3
"""Fix BidSell/MS mixing and multi-account status issues in okx-outcomes-bot."""

# Fix 1: Backend - control panel strategy detection
with open("backend/app.py") as f:
    content = f.read()

# Fix running_strategy to differentiate Bid-Sell and include Sniper
old1 = '''        running_strategy = None
        if inst.strategy_d.running:
            running_strategy = "Manual-D"
        elif inst.strategy_s.running:
            running_strategy = "Manual-S"'''

new1 = '''        running_strategy = None
        if inst.strategy_d.running:
            running_strategy = "Manual-D"
        elif inst.strategy_s.running:
            mode = getattr(inst.strategy_s.state, "mode", "classic")
            running_strategy = "Bid-Sell" if mode == "bid-sell" else "Manual-S"
        elif inst.sniper.running:
            running_strategy = "Sniper"'''

if old1 in content:
    content = content.replace(old1, new1, 1)
    print("✓ Fixed running_strategy differentiation")
else:
    print("✗ Could not find running_strategy block")

# Fix running field to include sniper
old2 = '"running": inst.strategy_d.running or inst.strategy_s.running,'
new2 = '"running": inst.strategy_d.running or inst.strategy_s.running or inst.sniper.running,'

if old2 in content:
    content = content.replace(old2, new2, 1)
    print("✓ Fixed running field to include sniper")
else:
    print("✗ Could not find running field")

with open("backend/app.py", "w") as f:
    f.write(content)

print("\nBackend fixed. Now fixing frontend...")

# Fix 2: Frontend - poll() logic for MS/BS separation
with open("frontend/dashboard.html") as f:
    html = f.read()

# The core issue: poll() sets MSR=true unconditionally, then always updates MS tab
# even when bid-sell is running. We need to split the display logic.

# Current problematic pattern in poll():
# MSR=s.manual_s_running||false;
# ...
# if(MSR){ <updates MS tab> ... if(s.manual_s_mode==='bid-sell'){BSR=true; <updates BS tab>} }
# else{ <resets MS tab> ... if(BSR){BSR=false; <resets BS tab>} }

# The fix: When mode is bid-sell, DON'T show MS tab as running.
# We'll use a separate approach: set MSR based on mode

old_msr = "MR=s.manual_running||false;PR=s.sniper_running||false;MSR=s.manual_s_running||false;"
new_msr = "MR=s.manual_running||false;PR=s.sniper_running||false;MSR=s.manual_s_running&&s.manual_s_mode!=='bid-sell';BSR=s.manual_s_running&&s.manual_s_mode==='bid-sell';"

if old_msr in html:
    html = html.replace(old_msr, new_msr, 1)
    print("✓ Fixed MSR/BSR assignment in poll()")
else:
    print("✗ Could not find MSR assignment in poll()")

# Now we need to handle the BSR display section.
# Currently BSR display is INSIDE the if(MSR) block.
# After the fix, MSR=false when bid-sell runs, so BSR display won't execute.
# We need to add a separate if(BSR) block.

# Find the end of the if(MSR){...}else{...} block and the start of if(PR)
# The current structure is:
# if(MSR){...if(s.manual_s_mode==='bid-sell'){BSR=true;...}}else{...if(BSR){BSR=false;...}}
# We need to restructure to:
# if(MSR){...classic MS display...}else{...reset MS...}
# if(BSR){...bid-sell display...}else if(!s.manual_s_running){...reset BS...}

# Let's find the exact code segments. The poll() is minified on one line, so we need
# to work with the exact substrings.

# Remove BSR handling from inside the if(MSR) block
# Current: if(s.manual_s_mode==='bid-sell'&&s.manual_s_asset_id){...}
#          if(s.manual_s_price&&...)document.getElementById('ms-price').value=...
#          if(s.manual_s_mode==='bid-sell'){BSR=true;...lockBS();...}
# The bid-sell part inside MSR block needs to be removed and put separately

# Old MSR block inner content that sets BS state:
old_bs_inside_msr = "if(s.manual_s_mode==='bid-sell'&&s.manual_s_asset_id){var _bsYes=s.manual_s_yes_asset_id||s.manual_s_asset_id;document.getElementById('bsmkt').value=_bsYes;var _bs=_mktMap[_bsYes];if(_bs)document.getElementById('bsmkt-btn').textContent=(_bs.question||_bs.event_title).substring(0,55);}if(s.manual_s_price&&document.activeElement!==document.getElementById('ms-price'))document.getElementById('ms-price').value=s.manual_s_price;if(s.manual_s_mode==='bid-sell'){BSR=true;document.getElementById('bsb').className='btn-stop';document.getElementById('bsb').textContent='Stop Bid-Sell';document.getElementById('bss').textContent='Running '+(s.manual_s_side||'')+' B'+(s.manual_s_price||'')+'/S'+(s.manual_s_sell_price||'');document.getElementById('bss').style.color='var(--green)';lockBS();if(s.manual_s_sell_price&&document.activeElement!==document.getElementById('bs-price'))document.getElementById('bs-price').value=s.manual_s_sell_price;}"

if old_bs_inside_msr in html:
    # Remove this from inside the MSR block - just keep the ms-price setting for classic mode
    html = html.replace(old_bs_inside_msr, "if(s.manual_s_price&&document.activeElement!==document.getElementById('ms-price'))document.getElementById('ms-price').value=s.manual_s_price;", 1)
    print("✓ Removed BS display from inside MSR block")
else:
    print("✗ Could not find BS-inside-MSR block")

# Now fix the else{} block that resets BSR when MSR is false
# Current: }else{...if(BSR){BSR=false;...unlockBS();}}
# Since BSR is now set independently, we don't need to reset it here
old_bsr_reset = "if(BSR){BSR=false;document.getElementById('bsb').className='btn-go';document.getElementById('bsb').textContent='Start Bid-Sell';document.getElementById('bss').textContent='Idle';document.getElementById('bss').style.color='var(--muted)';unlockBS();}"
if old_bsr_reset in html:
    html = html.replace(old_bsr_reset, "", 1)
    print("✓ Removed BSR reset from inside MSR else block")
else:
    print("✗ Could not find BSR reset in MSR else block")

# Now add a separate if(BSR) block after the MSR block.
# The if(PR) block starts with "if(PR){document.getElementById('pb')"
# We'll insert the BSR block just before it.

bs_block = """if(BSR){document.getElementById('bsb').className='btn-stop';document.getElementById('bsb').textContent='Stop Bid-Sell';var _bsYes=s.manual_s_yes_asset_id||s.manual_s_asset_id;if(_bsYes){document.getElementById('bsmkt').value=_bsYes;var _bs=_mktMap[_bsYes];if(_bs)document.getElementById('bsmkt-btn').textContent=(_bs.question||_bs.event_title).substring(0,55);_obNoSide=(s.manual_s_side==='no');if(!_okxAsset)sob(_bsYes);}document.getElementById('bss').textContent='Running '+(s.manual_s_side||'')+' B'+(s.manual_s_price||'')+'/S'+(s.manual_s_sell_price||'');document.getElementById('bss').style.color='var(--green)';lockBS();if(s.manual_s_sell_price&&document.activeElement!==document.getElementById('bs-price'))document.getElementById('bs-price').value=s.manual_s_sell_price;}else{document.getElementById('bsb').className='btn-go';document.getElementById('bsb').textContent='Start Bid-Sell';document.getElementById('bss').textContent='Idle';document.getElementById('bss').style.color='var(--muted)';unlockBS();}"""

# Insert before if(PR)
pr_start = "if(PR){document.getElementById('pb').className='btn-stop';"
if pr_start in html:
    html = html.replace(pr_start, bs_block + pr_start, 1)
    print("✓ Added separate BSR display block before PR block")
else:
    print("✗ Could not find PR block start")

# Also fix: tgBS sets MSR=true on start and MSR=false on stop.
# Since BSR and MSR are now independent display flags, tgBS should only manage BSR.
# But BSR needs MSR=true for internal logic (shared backend endpoint, monitor prefix, etc.)
# Actually no - after the poll fix, BSR is its own flag. Let's check what _activeMonPrefix does:
# function _activeMonPrefix(){if(MR)return'md-mon-';if(BSR)return'bs-mon-';if(MSR)return'ms-mon-';return null;}
# Good - BSR is checked before MSR, so monitor prefix works correctly.

# But tgBS() sets MSR=true on start:
old_tgbs_start = "BSR=true;MSR=true;"
# We no longer need to set MSR=true when BS starts (poll will set BSR from backend)
if old_tgbs_start in html:
    html = html.replace(old_tgbs_start, "BSR=true;", 1)
    print("✓ Fixed tgBS start (removed MSR=true)")
else:
    print("✗ Could not find tgBS MSR=true")

# tgBS stop sets MSR=false:
old_tgbs_stop = "BSR=false;MSR=false;"
if old_tgbs_stop in html:
    html = html.replace(old_tgbs_stop, "BSR=false;", 1)
    print("✓ Fixed tgBS stop (removed MSR=false)")
else:
    print("✗ Could not find tgBS MSR=false")

# Fix cxAll and flashClose which reset both:
old_cx = "MR=false;MSR=false;PR=false;"
new_cx = "MR=false;MSR=false;BSR=false;PR=false;"
# This appears twice (cxAll and flashClose)
count = html.count(old_cx)
if count >= 2:
    html = html.replace(old_cx, new_cx)
    print(f"✓ Fixed cxAll/flashClose BSR reset ({count} occurrences)")
else:
    print(f"✗ cxAll/flashClose: found {count} occurrences of reset pattern")

# Fix the tab switching logic that checks running state
# Line 545: var thisRunning=(n===0&&MR)||(n===1&&MSR)||(n===2&&PR);
old_tab = "var thisRunning=(n===0&&MR)||(n===1&&MSR)||(n===2&&PR);"
new_tab = "var thisRunning=(n===0&&MR)||(n===1&&MSR)||(n===4&&BSR)||(n===2&&PR);"
if old_tab in html:
    html = html.replace(old_tab, new_tab, 1)
    print("✓ Fixed tab switching running detection (added BSR for tab 4)")
else:
    print("✗ Could not find tab running check")

# Fix: if(!MR&&!MSR&&!PR){disconnectBook(true);}
old_disc = "if(!MR&&!MSR&&!PR){disconnectBook(true);}"
new_disc = "if(!MR&&!MSR&&!BSR&&!PR){disconnectBook(true);}"
html = html.replace(old_disc, new_disc)
print(f"✓ Fixed disconnectBook checks (added BSR)")

# Fix stopPolyOb check
old_spob = "else if(!MR&&!MSR&&!PR){disconnectBook(true);}"
new_spob = "else if(!MR&&!MSR&&!BSR&&!PR){disconnectBook(true);}"
if old_spob in html:
    html = html.replace(old_spob, new_spob)
    print("✓ Fixed stopPolyOb disconnectBook check")

with open("frontend/dashboard.html", "w") as f:
    f.write(html)

print("\n✅ All fixes applied!")
