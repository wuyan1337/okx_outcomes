import re
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "frontend" / "dashboard.html"


class DashboardAccountDeleteTests(unittest.TestCase):
    def test_deleted_account_with_incomplete_file_cleanup_leaves_account_view(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is required for dashboard JavaScript tests")
        html = DASHBOARD.read_text(encoding="utf-8")
        match = re.search(
            r"async function confirmDeleteAcct\(\)\{.*?\}\n",
            html,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        script = f"""
let alerts=[];
let closes=[];
let window={{location:{{hash:'#/account-a'}}}};
let currentAcctId='account-a';
function confirm(){{return true;}}
function alert(value){{alerts.push(value);}}
function closeModal(value){{closes.push(value);}}
async function gapi(){{return {{json:async()=>({{
  ok:true,deleted:true,cleanup_complete:false,warning:'cleanup warning'
}})}};}}
{match.group(0)}
(async()=>{{
  await confirmDeleteAcct();
  if(window.location.hash!=='#/')throw new Error('did not leave deleted account');
  if(closes.length!==1)throw new Error('modal was not closed');
  if(alerts[0]!=='cleanup warning')throw new Error('cleanup warning was not shown');
}})().catch((error)=>{{console.error(error);process.exit(1);}});
"""
        completed = subprocess.run(
            [node, "-e", script],
            cwd=ROOT,
            text=True,
            encoding="utf-8",
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
