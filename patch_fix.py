
import re

with open("backend/services/auto_rebalance.py", "r") as f:
    content = f.read()

old = "            # Step 4: Sync cost layers with actual positions\n            await self._sync_positions()"

new = """            # Step 4: Sync cost layers with actual positions
            # Guard: if we just captured partial fills, don't let sync wipe them
            # (OKX position API may lag behind cancel/fill events)
            layers_before_sync = sum(self.cost_layers.values())
            await self._sync_positions()
            layers_after_sync = sum(self.cost_layers.values())
            if layers_before_sync > 0 and layers_after_sync <= 0:
                logger.warning(f"[Rebalance:{self.account_id}] sync wiped {layers_before_sync:.0f} units - API lag, restoring")
                # Restore: re-add what _capture_partial_fills found
                self.add_to_layer(old_price_cents, layers_before_sync)"""

if old in content:
    content = content.replace(old, new)
    with open("backend/services/auto_rebalance.py", "w") as f:
        f.write(content)
    print("PATCHED OK")
else:
    print("OLD STRING NOT FOUND")
    idx = content.find("Step 4")
    if idx >= 0:
        print(repr(content[idx:idx+100]))
