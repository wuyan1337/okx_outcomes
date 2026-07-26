import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "frontend" / "dashboard.html"
APP = ROOT / "backend" / "app.py"


def extract_function(source: str, name: str) -> str:
    markers = (f"function {name}(", f"async function {name}(")
    starts = [source.find(marker) for marker in markers]
    start = min(index for index in starts if index >= 0)
    brace = source.find("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    raise AssertionError(f"Unclosed JavaScript function: {name}")


class DashboardSniperActiveTargetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = DASHBOARD.read_text(encoding="utf-8")

    def test_active_target_helpers_switch_pair_side_and_price(self):
        if not shutil.which("node"):
            self.skipTest("Node.js is not installed")
        helpers = "\n".join(
            extract_function(self.html, name)
            for name in (
                "sniperTargetFromState",
                "bindSniperBookTarget",
                "sniperTargetPrice",
                "sniperOkxBestPrice",
                "renderOkxOb",
            )
        )
        script = f"""
const assert=require('assert');
let _snTargets=[
  {{id:'first',poly_token_id:'poly-1',yes_asset_id:'yes-1',okx_side:'YES'}},
  {{id:'second',poly_token_id:'poly-2',yes_asset_id:'yes-2',okx_side:'NO'}},
  {{id:'third',poly_token_id:'poly-3',yes_asset_id:'yes-3',okx_side:'YES'}}
];
let _snBoundTargetId='',_snBoundPolyToken='',_snBoundYesAsset='',_snBoundSide='';
let _obNoSide=false,_bookWs=null,_polyToken='',_okxAsset='',_cachedOkxBids=[['0.999','4']],_cachedOkxAsks=[['0.70','5']];
let calls=[];
let elements={{'ob-id':{{}},'ob-b':{{}},'ob-a':{{}}}},formatted=[];
let document={{getElementById:(id)=>elements[id]}};
function formatOkxBookPrice(value){{formatted.push(Number(value));return String(value);}}
function connectBook(poly,asset,force){{calls.push([poly,asset,force]);_polyToken=poly;_okxAsset=asset;_bookWs={{readyState:1}};}}
function renderPolyOb(){{}}
{helpers}
let state={{active_target_id:'second',target_prices:{{first:'0.11',second:'0.42',third:'0.73'}}}};
let active=sniperTargetFromState(state);
assert.equal(active.id,'second');
assert.equal(sniperTargetPrice(state,active),'0.42');
bindSniperBookTarget(active);
assert.deepEqual(calls,[['poly-2','yes-2',true]]);
assert.equal(_obNoSide,true);
assert(Math.abs(sniperOkxBestPrice(active)-0.001)<1e-12);
renderOkxOb();
assert.equal(elements['ob-id'].textContent,'Live orderbook (NO)');
assert(formatted.some((value)=>Math.abs(value-0.30)<1e-12));
assert(formatted.some((value)=>Math.abs(value-0.001)<1e-12));
bindSniperBookTarget(active);
assert.equal(calls.length,1);
// Any stale callback that rebinds an old target must be repaired.
_polyToken='poly-1';_okxAsset='yes-1';_obNoSide=false;
bindSniperBookTarget(active);
assert.deepEqual(calls[1],['poly-2','yes-2',true]);
assert.equal(_obNoSide,true);
bindSniperBookTarget(_snTargets[2]);
assert.deepEqual(calls[2],['poly-3','yes-3',true]);
assert.equal(_obNoSide,false);
"""
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_websocket_rejects_stale_callbacks_and_forces_target_rebind(self):
        connect_book = extract_function(self.html, "connectBook")
        sync_status = extract_function(self.html, "syncSniperStatus")
        tab_switch = extract_function(self.html, "stab")
        self.assertIn("generation!==_bookGeneration", connect_book)
        self.assertIn("socket!==_bookWs", connect_book)
        self.assertIn("bindSniperBookTarget(bookTarget)", sync_status)
        self.assertIn("sniperTargetPrice(d,displayed)", sync_status)
        self.assertIn("requestSeq!==_snStatusRequestSeq", sync_status)
        self.assertIn("state==='error'", sync_status)
        self.assertNotIn("document.getElementById('pi').value=''", tab_switch)
        self.assertNotIn("d.last_poly_price", sync_status)

    def test_poly_resolver_and_stop_handle_stale_or_locked_results(self):
        resolver = extract_function(self.html, "onPolyInput")
        toggle = extract_function(self.html, "tgSniper")
        sync_status = extract_function(self.html, "syncSniperStatus")
        self.assertIn("requestSeq!==_snPolyResolveSeq", resolver)
        self.assertIn("value.trim()!==v", resolver)
        self.assertIn("stopData.locked", toggle)
        self.assertIn("!stopData.ok", toggle)
        self.assertIn("d.persistence_error", sync_status)

    def test_rebalance_toggle_restores_persisted_server_state_on_failure(self):
        toggle = extract_function(self.html, "toggleRebalance")
        sync_ui = extract_function(self.html, "syncRebalUI")
        disabled = extract_function(self.html, "setRebalanceDisabled")
        poll = extract_function(self.html, "poll")
        self.assertIn("typeof d.enabled==='boolean'", toggle)
        self.assertIn("/rebalance/status", toggle)
        self.assertIn("isCurrentAccountView", toggle)
        self.assertIn("setRebalanceDisabled(true)", toggle)
        self.assertIn(
            "requestSeq===_rebalToggleSeq&&isCurrentAccountView",
            toggle,
        )
        self.assertIn("bs-mon-rebal", sync_ui)
        self.assertIn("bs-mon-rebal", disabled)
        self.assertIn("rebalAccountAtRequest=currentAcctId", poll)
        self.assertIn(
            "isCurrentAccountView(rebalAccountAtRequest,rebalNavAtRequest)",
            poll,
        )
        self.assertIn("rebalToggleSeq!==_rebalToggleSeq", poll)

    def test_new_sniper_has_no_legacy_single_market_scaffolding(self):
        poll = extract_function(self.html, "poll")
        for stale_id in (
            'id="pmkt"',
            'id="pmkt-btn"',
            'id="pmkt-list"',
            'id="pmkt-q"',
            'id="sdrop-pmkt"',
            'id="poly-sel"',
        ):
            self.assertNotIn(stale_id, self.html)
        self.assertNotIn("_polyYesToken", self.html)
        self.assertNotIn("s.sniper_yes_token", poll)
        self.assertNotIn("s.sniper_yes_asset", poll)
        self.assertNotRegex(self.html, r"\bSA\b")
        self.assertNotRegex(self.html, r"\bobI\b")
        self.assertNotIn("_polyResolvedToken", self.html)
        self.assertNotIn("function startMonitor(", self.html)
        self.assertNotIn("function stopMonitor(", self.html)
        self.assertIn("oldStatusSeq!==_soStatusSeq", poll)
        self.assertIn(
            "isCurrentAccountView(oldAccountAtRequest,oldNavAtRequest)", poll
        )

    def test_account_actions_are_bound_to_the_captured_account(self):
        cancel_all = extract_function(self.html, "cxAll")
        load_config = extract_function(self.html, "loadFullCfg")
        save_config = extract_function(self.html, "saveAcctCfg")
        self.assertNotIn("/api/strategy/stop", cancel_all)
        self.assertEqual(cancel_all.count("gapi("), 1)
        self.assertIn("requestSeq=++_cfgLoadSeq", load_config)
        self.assertIn("d.error||d.detail", save_config)

        if not shutil.which("node"):
            self.skipTest("Node.js is not installed")
        script = f"""
const assert=require('assert');
let currentAcctId='acct-a',navId=1,_cfgLoadSeq=0;
let MR=true,MSR=true,BSR=true,PR=true,SOR=true;
let requests=[],events=[],timers=[],responders=[];
let elements={{}};
['cfg-ak','cfg-sk','cfg-pp','cfg-ag','cfg-region'].forEach((id)=>elements[id]={{value:''}});
let document={{getElementById:(id)=>elements[id]}};
function isCurrentAccountView(account,navigation){{return currentAcctId===account&&navId===navigation;}}
function al(type,message){{events.push([type,message]);}}
function closeModal(id){{events.push(['CLOSE',id]);}}
function poll(){{events.push(['POLL']);}}
function setTimeout(fn){{timers.push(fn);}}
function gapi(url,options){{requests.push([url,options]);return responders.shift()();}}
function deferred(){{let resolve;let promise=new Promise((done)=>{{resolve=done;}});return{{promise,resolve}};}}
{cancel_all}
{load_config}
{save_config}
(async()=>{{
  let cancel=deferred();responders.push(()=>cancel.promise);
  let cancelRun=cxAll();
  assert.equal(requests[0][0],'/api/accounts/acct-a/cancel_all');
  assert.equal(requests.length,1);
  currentAcctId='acct-b';navId=2;
  cancel.resolve({{ok:true,json:async()=>({{ok:true}})}});
  await cancelRun;
  assert.equal(MR,true);assert.equal(MSR,true);assert.equal(BSR,true);
  assert.equal(PR,true);assert.equal(SOR,true);assert.equal(events.length,0);

  currentAcctId='acct-a';navId=3;
  let first=deferred(),second=deferred();
  responders.push(()=>first.promise,()=>second.promise);
  let loadOne=loadFullCfg(),loadTwo=loadFullCfg();
  assert.equal(requests[1][0],'/api/accounts/acct-a/config');
  assert.equal(requests[2][0],'/api/accounts/acct-a/config');
  second.resolve({{ok:true,json:async()=>({{api_key:'new',secret_key:'s',passphrase:'p',agent_key:'g',region:'CN'}})}});
  await loadTwo;
  first.resolve({{ok:true,json:async()=>({{api_key:'old',region:'AU'}})}});
  await loadOne;
  assert.equal(elements['cfg-ak'].value,'new');
  assert.equal(elements['cfg-region'].value,'CN');

  elements['cfg-ak'].value='key';elements['cfg-sk'].value='secret';
  elements['cfg-pp'].value='pass';elements['cfg-ag'].value='agent';
  responders.push(()=>Promise.resolve({{ok:false,status:409,json:async()=>({{ok:false,error:'busy'}})}}));
  await saveAcctCfg();
  assert.equal(requests[3][0],'/api/accounts/acct-a/config');
  assert(events.some((entry)=>entry[0]==='ERROR'&&entry[1].includes('busy')));

  let before=events.length,save=deferred();responders.push(()=>save.promise);
  let saveRun=saveAcctCfg();currentAcctId='acct-b';navId=4;
  save.resolve({{ok:true,status:200,json:async()=>({{ok:true}})}});
  await saveRun;
  assert.equal(events.length,before);
}})().catch((error)=>{{console.error(error);process.exit(1);}});
"""
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_persisted_okx_target_remains_visible_before_markets_load(self):
        options = extract_function(self.html, "snSelectOptions")
        render = extract_function(self.html, "renderSniperTargets")
        self.assertIn("var okxItems=_mkts.slice()", render)
        self.assertIn("yes_asset_id:t.yes_asset_id", render)

        if not shutil.which("node"):
            self.skipTest("Node.js is not installed")
        script = f"""
const assert=require('assert');
let box={{innerHTML:'',querySelectorAll:()=>[]}};
let document={{getElementById:(id)=>id==='sn-targets'?box:null}};
let _snPolyOptions=[],_mkts=[],_snActiveTargetId='',PR=false,_snLocked=false;
let _snTargets=[{{id:'target-1',poly_token_id:'poly-1',poly_outcome:'Argentina',yes_asset_id:'yes-1',okx_market:'Argentina market',okx_side:'YES'}}];
function esc(value){{return String(value);}}
function setSniperLocked(){{}}
{options}
{render}
renderSniperTargets();
assert(box.innerHTML.includes('value="yes-1" selected>Argentina market</option>'));
_snTargets[0].yes_asset_id='yes-2';_snTargets[0].okx_market='';
renderSniperTargets();
assert(box.innerHTML.includes('value="yes-2" selected>yes-2</option>'));
"""
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_manual_price_guards_and_xp_precision(self):
        tg_bid_sell = extract_function(self.html, "tgBS")
        manual_sell = extract_function(self.html, "mtSell")
        self.assertNotIn("Math.max(0.1,sellPrice-1)", tg_bid_sell)
        self.assertIn("buyPrice<0.1", tg_bid_sell)
        self.assertIn("isValidOkxCents(buyPrice)", tg_bid_sell)
        self.assertIn("isValidOkxCents(sellPrice)", tg_bid_sell)
        self.assertIn("(price/100)*Number(size)<1", manual_sell)
        self.assertIn("Number(size)<Number(_mtPosSize)", manual_sell)
        self.assertIn("formatOkxBookPrice(d.holding.buy_price)", self.html)
        self.assertIn("formatOkxBookPrice(d.holding.sell_target)", self.html)

        if not shutil.which("node"):
            self.skipTest("Node.js is not installed")
        formatter = extract_function(self.html, "formatXpValue")
        poly_formatter = extract_function(self.html, "formatPolyPrice")
        result = subprocess.run(
            [
                "node",
                "-e",
                f"{formatter};{poly_formatter};"
                "const a=require('assert');"
                "a.equal(formatXpValue(0.001),'0.001');"
                "a.equal(formatXpValue(1.230000),'1.23');"
                "a.equal(formatXpValue(1000),'1000');"
                "a.equal(formatPolyPrice(0.01005),'1.01¢');"
                "a.equal(formatPolyPrice(0.00015),'0.02¢');"
                "a.equal(formatPolyPrice('invalid'),'--');",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_backend_enforces_bid_sell_spread_and_partial_sell_notional(self):
        app_source = APP.read_text(encoding="utf-8")
        self.assertIn(
            'sell_price_decimal - buy_price_decimal != as_decimal("0.01")',
            app_source,
        )
        self.assertIn("actual_price_decimal * size_decimal < 1", app_source)
        self.assertIn("size_decimal != available_size", app_source)
        self.assertIn("full available position", app_source)


if __name__ == "__main__":
    unittest.main()
