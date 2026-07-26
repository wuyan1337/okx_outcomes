import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "frontend" / "dashboard.html"


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


class DashboardLatencyUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = DASHBOARD.read_text(encoding="utf-8")

    def run_node(self, script: str):
        if not shutil.which("node"):
            self.skipTest("Node.js is not installed")
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            encoding="utf-8",
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_latency_checks_are_region_aware_batch_requests(self):
        add_check = extract_function(self.html, "refreshAddLatency")
        health_check = extract_function(self.html, "runHealth")
        health_path = extract_function(self.html, "healthLatencyPath")
        health_render = extract_function(self.html, "renderHealthLatency")
        add_render = extract_function(self.html, "renderAddLatency")

        self.assertIn("region='+encodeURIComponent(region)+'&samples=5", add_check)
        self.assertIn("healthLatencyPath(region)", health_check)
        self.assertIn("yes_asset_id", health_path)
        self.assertIn("account_id", health_path)
        self.assertIn("poly_token_id", health_path)
        self.assertIn("_currentAccountRegion===region", health_path)
        self.assertNotIn("/api/proxy/latency/", health_check)
        self.assertIn("data.poly_direct", health_render)
        self.assertIn("execution_path.status==='not_measured'", health_render)
        self.assertIn("row.qualityRank===0", health_render)
        self.assertIn("row.qualityRank===0", add_render)
        self.assertNotIn("total_ms", self.html)
        self.assertNotIn("jp2_ms", self.html)
        self.assertIn("[LOWEST RTT]", self.html)
        self.assertNotIn("[FASTEST]", self.html)

    def test_health_uses_account_health_and_canonical_node_labels(self):
        health_check = extract_function(self.html, "runHealth")
        control_poll = extract_function(self.html, "pollCtrl")
        add_render = extract_function(self.html, "renderAddLatency")

        self.assertIn("'/health'", health_check)
        self.assertNotIn("'/balance'", health_check)
        self.assertIn("health.healthy===true", health_check)
        self.assertIn("a.proxy_label", health_check)
        self.assertIn("a.proxy_label||a.proxy_node", control_poll)
        self.assertIn("row.label", add_render)
        self.assertIn('id="health-region"', self.html)
        self.assertIn('onchange="refreshAddLatency()"', self.html)
        self.assertIn('onchange="healthRegionChanged()"', self.html)

    def test_abort_and_late_response_guards_cover_both_modals(self):
        health_check = extract_function(self.html, "runHealth")
        add_check = extract_function(self.html, "refreshAddLatency")
        close_all = extract_function(self.html, "closeAllModals")

        self.assertGreaterEqual(health_check.count("signal:controller.signal"), 4)
        self.assertGreaterEqual(add_check.count("signal:controller.signal"), 2)
        self.assertIn(
            "_healthAbort===controller&&_healthRunSeq===seq", health_check
        )
        self.assertIn(
            "_addLatencyAbort===controller&&_addLatencySeq===seq", add_check
        )
        self.assertIn("cancelHealthRun()", close_all)
        self.assertIn("cancelAddLatency()", close_all)

        helpers = "\n".join(
            extract_function(self.html, name)
            for name in ("cancelHealthRun", "isHealthRunActive")
        )
        self.run_node(
            f"""
const assert=require('assert');
let _healthRunSeq=0,_healthAbort=null;
let modal={{classList:{{contains:(name)=>name==='show'}}}};
let document={{getElementById:()=>modal}};
{helpers}
cancelHealthRun();
let firstSeq=_healthRunSeq,first=new AbortController();_healthAbort=first;
assert(isHealthRunActive(firstSeq,first));
cancelHealthRun();
assert(first.signal.aborted);
let secondSeq=_healthRunSeq,second=new AbortController();_healthAbort=second;
assert(!isHealthRunActive(firstSeq,first));
assert(isHealthRunActive(secondSeq,second));
if(_healthAbort===first&&_healthRunSeq===firstSeq)_healthAbort=null;
assert.strictEqual(_healthAbort,second);
"""
        )

    def test_late_health_run_cannot_clobber_the_new_controller(self):
        helpers = "\n".join(
            extract_function(self.html, name)
            for name in ("cancelHealthRun", "isHealthRunActive", "runHealth")
        )
        self.run_node(
            f"""
const assert=require('assert');
let _healthRunSeq=0,_healthAbort=null;
let results={{textContent:'',innerHTML:'',insertAdjacentHTML(){{}}}};
let button={{disabled:false,textContent:'Run Check'}};
let region={{value:'AU'}};
let modal={{classList:{{contains:(name)=>name==='show'}}}};
let elements={{healthResults:results,healthRunBtn:button,'health-region':region,healthModal:modal}};
let document={{getElementById:(id)=>elements[id]}};
function deferred(){{let resolve;let promise=new Promise((done)=>resolve=done);return{{promise,resolve}};}}
let first=deferred(),second=deferred(),calls=0,signals=[];
function latencyFetchJson(path,options){{signals.push(options.signal);return(calls++===0?first:second).promise;}}
function healthLatencyPath(){{return'/batch';}}
function renderHealthLatency(){{return'fresh';}}
function isAbortedRequest(error,controller){{return controller.signal.aborted;}}
function esc(value){{return String(value);}}
{helpers}
(async()=>{{
  let oldRun=runHealth(),oldController=_healthAbort;
  let newRun=runHealth(),newController=_healthAbort;
  assert(oldController.signal.aborted);
  assert.notStrictEqual(oldController,newController);
  first.resolve({{region:'AU',latencies:{{}}}});
  await oldRun;
  assert.strictEqual(_healthAbort,newController);
  assert.equal(button.disabled,true);
  assert.equal(button.textContent,'Checking...');
  assert.strictEqual(signals[0],oldController.signal);
  assert.strictEqual(signals[1],newController.signal);
  cancelHealthRun();second.resolve({{region:'AU',latencies:{{}}}});
  await newRun;
}})().catch((error)=>{{console.error(error);process.exit(1);}});
"""
        )

    def test_late_add_account_region_result_cannot_replace_the_new_region(self):
        helpers = "\n".join(
            extract_function(self.html, name)
            for name in ("cancelAddLatency", "isAddLatencyActive", "refreshAddLatency")
        )
        self.run_node(
            f"""
const assert=require('assert');
let _addLatencySeq=0,_addLatencyAbort=null;
let modal={{classList:{{contains:(name)=>name==='show'}}}};
let select={{value:'node-a',disabled:false}};
let info={{textContent:'',renderedRegion:''}};
let region={{value:'AU'}};
let elements={{addModal:modal,'aa-proxy':select,latInfo:info,'aa-region':region}};
let document={{getElementById:(id)=>elements[id]}};
function deferred(){{let resolve;let promise=new Promise((done)=>resolve=done);return{{promise,resolve}};}}
let au=deferred(),cn=deferred(),signals=[];
function latencyFetchJson(path,options){{
  signals.push(options.signal);
  if(path==='/api/accounts/proxies')return Promise.resolve({{proxies:[{{id:'node-a',label:'Node A'}}]}});
  return path.includes('region=AU')?au.promise:cn.promise;
}}
function latencyMetricView(){{return{{ok:false,median:null}};}}
function populateAddProxyOptions(){{}}
function sortedLatencyEntries(_latencies,nodes){{return nodes.map((node)=>({{id:node.id,label:node.label,view:{{ok:true,median:1}},entry:{{}}}}));}}
function renderAddLatency(data){{info.renderedRegion=data.region;}}
function isAbortedRequest(error,controller){{return controller.signal.aborted;}}
{helpers}
(async()=>{{
  let oldRun=refreshAddLatency(),oldController=_addLatencyAbort;
  await Promise.resolve();await Promise.resolve();
  region.value='CN';
  let newRun=refreshAddLatency(),newController=_addLatencyAbort;
  await Promise.resolve();await Promise.resolve();
  assert(oldController.signal.aborted);
  au.resolve({{region:'AU',latencies:{{}}}});await oldRun;
  assert.strictEqual(_addLatencyAbort,newController);
  assert.notEqual(info.renderedRegion,'AU');
  cn.resolve({{region:'CN',latencies:{{}}}});await newRun;
  assert.equal(info.renderedRegion,'CN');
  assert(signals.some((signal)=>signal===oldController.signal));
  assert(signals.some((signal)=>signal===newController.signal));
}})().catch((error)=>{{console.error(error);process.exit(1);}});
"""
        )

    def test_metric_rendering_sorting_and_real_sniper_timing(self):
        helpers = "\n".join(
            extract_function(self.html, name)
            for name in (
                "finiteLatencyMs",
                "formatLatencyMs",
                "latencyErrorText",
                "latencyMetricView",
                "sortedLatencyEntries",
                "sniperExecutionLatencyText",
            )
        )
        self.run_node(
            f"""
const assert=require('assert');
{helpers}
let market=latencyMetricView({{
  scope:'market_read',cold_ms:205,
  warm:{{requested:5,successes:5,failures:0,median_ms:82,min_ms:70,max_ms:110,jitter_ms:40}}
}});
assert.equal(market.median,82);
assert(market.text.includes('82ms warm'));
assert(market.text.includes('70ms–110ms'));
assert(market.text.includes('40ms jitter'));
assert(market.text.includes('205ms cold'));
assert(market.text.includes('5/5 samples'));
assert(market.text.includes('Market read'));
assert.equal(market.color,'var(--green)');
let connectivity=latencyMetricView({{
  scope:'connectivity_only',warm:{{requested:5,successes:5,failures:0,median_ms:40}}
}});
assert(connectivity.text.includes('Connectivity only'));
assert.equal(connectivity.color,'var(--yellow)');
let failed=latencyMetricView({{errors:[{{kind:'timeout',message:'timed out'}}]}});
assert.equal(failed.ok,false);assert(failed.text.includes('timed out'));
assert.equal(latencyMetricView({{warm:{{median_ms:null}}}}).ok,false);
let rows=sortedLatencyEntries({{
  slow:{{label:'Tokyo-02',okx:{{warm:{{median_ms:90}}}}}},
  dead:{{label:'Sydney-01',okx:{{errors:['offline']}}}},
  fast:{{label:'Tokyo-01',okx:{{warm:{{median_ms:30}}}}}}
}});
assert.deepEqual(rows.map((row)=>row.label),['Tokyo-01','Tokyo-02','Sydney-01']);
let failedWarm=sortedLatencyEntries({{node:{{okx_ms:25,okx:{{cold_ms:25,warm:{{median_ms:null}},errors:[{{message:'warm failed'}}]}}}}}})[0];
assert.equal(failedWarm.view.ok,false);
let failedPartial=sortedLatencyEntries({{
  unreliable:{{label:'Unreliable',okx_ms:4,okx:{{status:'failed',warm:{{requested:5,successes:2,failures:3,median_ms:4}},errors:[{{message:'mostly timed out'}}]}}}},
  healthy:{{label:'Healthy',okx:{{status:'ok',warm:{{requested:5,successes:5,failures:0,median_ms:30}}}}}}
}});
assert.deepEqual(failedPartial.map((row)=>row.label),['Healthy','Unreliable']);
assert.equal(failedPartial[1].view.ok,false);
assert(failedPartial[1].view.text.includes('mostly timed out'));
let degradedFirst=sortedLatencyEntries({{
  degraded:{{label:'Degraded',okx:{{status:'degraded',warm:{{requested:5,successes:3,failures:2,median_ms:5}}}}}},
  stable:{{label:'Stable',okx:{{status:'ok',warm:{{requested:5,successes:5,failures:0,median_ms:30}}}}}}
}});
assert.deepEqual(degradedFirst.map((row)=>row.label),['Stable','Degraded']);
assert.equal(degradedFirst[0].qualityRank,0);assert.equal(degradedFirst[1].qualityRank,1);
let skippedWarm=latencyMetricView({{status:'failed',warm:{{requested:5,attempted:0,skipped:5}},errors:[{{message:'cold failed'}}]}});
assert.equal(skippedWarm.ok,false);assert(skippedWarm.text.includes('5/5 warm skipped'));
let text=sniperExecutionLatencyText({{last:{{
  region:'AU',
  proxy_name:'JP-TYO-02',
  trigger_to_order_ack_ms:145.4,
  stages_ms:{{okx_book:30,positions:20,balance:15,order_submit:55}}
}},rolling:{{order_attempts:6,order_ack_samples:5,successful_order_responses:4,failed_order_responses:1,unacknowledged_order_attempts:1,attempt_success_rate:0.667,median_ms:130,min_ms:100,max_ms:170,jitter_ms:70}}}},'AU');
assert.equal(text,'Execution latency [AU · JP-TYO-02]: Trigger→Ack 145.4ms · Book 30ms · Position 20ms · Balance 15ms · Order 55ms · Rolling p50 130ms (100ms–170ms, jitter 70ms) · Order attempts 4/6 ok · unacked 1 · fail 33.3%');
assert.equal(sniperExecutionLatencyText({{last:null}},'CN'),'Execution latency [CN]: no sample');
assert.equal(sniperExecutionLatencyText({{last:{{region:'CN',trigger_to_order_ack_ms:null,stages_ms:{{}}}}}},'AU'),'Execution latency [CN]: no sample');
assert.equal(sniperExecutionLatencyText({{last:{{trigger_to_order_ack_ms:null,stages_ms:{{}}}}}},'AU'),'Execution latency [AU]: no sample');
"""
        )

    def test_health_market_assets_are_never_reused_cross_region(self):
        helper = extract_function(self.html, "healthLatencyPath")
        self.run_node(
            f"""
const assert=require('assert');
let currentAcctId='account-a',_currentAccountRegion='AU';
let _snActiveTargetId='target-b',_snTargets=[
  {{id:'target-a',yes_asset_id:'wrong asset',poly_token_id:'wrong/token'}},
  {{id:'target-b',yes_asset_id:'yes asset',poly_token_id:'poly/token'}}
];
{helper}
let same=healthLatencyPath('AU');
assert(same.includes('yes_asset_id=yes%20asset'));
assert(same.includes('account_id=account-a'));
assert(same.includes('poly_token_id=poly%2Ftoken'));
let other=healthLatencyPath('CN');
assert(!other.includes('yes_asset_id='));
assert(!other.includes('account_id='));
assert(!other.includes('poly_token_id='));
_snActiveTargetId='';
let ambiguous=healthLatencyPath('AU');
assert(!ambiguous.includes('yes_asset_id='));
assert(!ambiguous.includes('account_id='));
assert(!ambiguous.includes('poly_token_id='));
_snTargets=[{{id:'only',yes_asset_id:'single asset',poly_token_id:'single/token'}}];
let single=healthLatencyPath('AU');
assert(single.includes('yes_asset_id=single%20asset'));
assert(single.includes('poly_token_id=single%2Ftoken'));
_snTargets=[{{id:'incomplete',yes_asset_id:'asset only',poly_token_id:''}}];
let incomplete=healthLatencyPath('AU');
assert(!incomplete.includes('yes_asset_id='));
assert(!incomplete.includes('account_id='));
currentAcctId=null;
let global=healthLatencyPath('AU');
assert(!global.includes('yes_asset_id='));
assert(!global.includes('account_id='));
assert(!global.includes('poly_token_id='));
"""
        )

    def test_sniper_panel_reads_only_recorded_execution_stages(self):
        status = extract_function(self.html, "syncSniperStatus")
        formatter = extract_function(self.html, "sniperExecutionLatencyText")
        self.assertIn('id="sn-latency"', self.html)
        self.assertIn(
            "sniperExecutionLatencyText(d.latency,_currentAccountRegion)", status
        )
        for field in (
            "trigger_to_order_ack_ms",
            "okx_book",
            "positions",
            "balance",
            "order_submit",
            "order_attempts",
            "order_ack_samples",
            "successful_order_responses",
            "unacknowledged_order_attempts",
            "attempt_success_rate",
            "median_ms",
            "jitter_ms",
        ):
            self.assertIn(field, formatter)
        self.assertNotIn("total_ms", formatter)
        self.assertNotIn("reduce(", formatter)
        self.assertNotIn("pre_order_exits", formatter)
        self.assertNotIn("rolling.window", formatter)


if __name__ == "__main__":
    unittest.main()
