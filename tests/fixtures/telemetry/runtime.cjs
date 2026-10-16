// Chain: new ordinary method -> automatic wrapper; async owner -> original context/result; bounded detail/loss.
const assert=require('node:assert/strict'),{create}=require('../../../src/telemetry/runtime.js');
(async()=>{
 const rows=[],timers=new Map();let tick=0,timerId=0;
 const runtime=create({emit:r=>rows.push(r),id:()=>'a'.repeat(32),clock:()=>++tick,now:()=>123,
  schedule:f=>{timers.set(++timerId,f);return timerId;},cancel:id=>timers.delete(id)});
 const error={private:'PRIVATE ERROR'};
 const m=runtime.module('fixture',{added(x){return this.factor+x;},fails(){throw error;},nested(){return m.added.call({factor:1},2);}});
 assert.equal(m.added.call({factor:3},4),7);assert.equal(m.nested(),3);assert.throws(()=>m.fails(),e=>e===error);
 assert.equal(rows.length,0,'normal internal calls produced detail');assert.equal(tick,0,'normal methods read the clock');
 runtime.flush();const aggregates=rows.splice(0);assert.equal(aggregates.length,3);assert.equal(aggregates.find(r=>r.operation==='js.fixture.added').counts.calls,2);
 runtime.flush();assert.equal(rows.length,0,'flush counted calls twice');
 runtime.diagnose('js.fixture.',60000,2);m.nested();m.nested();assert.equal(rows.filter(r=>r.source==='diagnostic').length,2);
 assert.equal(runtime.health().diagnostic.remaining,0);
 runtime.diagnose('*',60000,1);runtime.action('click-quota',()=>0)();assert.equal(runtime.health().diagnostic.remaining,1,'action consumed internal detail quota');m.nested();assert.equal(runtime.health().diagnostic.remaining,0);
 runtime.diagnose('*',1,1);const count=rows.length;m.nested();assert.equal(rows.length,count,'expired detail admitted a method');
 assert.throws(()=>runtime.module('invalid',{async detached(){}}));
 assert.throws(()=>runtime.effect('invalid',()=>1,async()=>{}));
 assert.throws(()=>runtime.diagnose('*',60001,256));
 rows.length=0;
 let resolveA,resolveB;const a=new Promise(r=>resolveA=r),b=new Promise(r=>resolveB=r),received=[];
 const start=runtime.action('click',()=>{
   const origin=runtime.context();
   runtime.effect('command',()=>{assert.equal(runtime.context().trace,origin.trace);return a;},r=>{received.push([r,runtime.context()]);});
   return origin;
 });
 const origin=start();assert.equal(runtime.context(),null);
 runtime.action('unrelated',()=>runtime.effect('second',()=>b,()=>{}))();
 resolveB(2);await Promise.resolve();resolveA(1);await Promise.resolve();await Promise.resolve();
 assert.equal(received.length,1);assert.equal(received[0][0].value,1);assert.equal(received[0][1].trace,origin.trace);
 assert.equal(runtime.context(),null);assert.equal(runtime.health().pending,0);assert.equal(timers.size,0);
 assert(!JSON.stringify(rows).includes('PRIVATE'));assert(!JSON.stringify(rows).includes('value'));
 let late;runtime.action('wait',()=>runtime.effect('slow',()=>new Promise(r=>late=r),r=>received.push([r,runtime.context()])))();
 [...timers.values()][0]();assert.equal(received.at(-1)[0].unknown,true);assert.equal(runtime.health().pending,1,'timeout abandoned ownership');
 late('private value');await Promise.resolve();await Promise.resolve();assert.equal(runtime.health().pending,0);assert.equal(received.at(-1)[0].value,'private value');
 const broken=create({emit:()=>{throw error;},id:()=>'b'.repeat(32)});
 assert.equal(broken.action('still-works',()=>42)(),42);assert.equal(broken.health().lost,2);
 let accepting=false;const recovered=[];const retry=create({emit:r=>accepting?recovered.push(r):false,id:()=>'c'.repeat(32)});
 const helper=retry.module('recovery',{call(){return 1;}});helper.call();retry.flush();assert.equal(retry.health().lost,1);accepting=true;retry.flush();assert.equal(recovered[0].counts.calls,1);retry.flush();assert.equal(recovered.length,1);
 console.log('PASS wrapped new methods, receiver/throw identity, cheap normal mode, detail cap, async ownership/context, late result and loss');
})().catch(e=>{console.error(e);process.exitCode=1;});
