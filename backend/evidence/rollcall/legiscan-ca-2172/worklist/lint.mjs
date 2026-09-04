// Plain-language lint for a judgments.json file. Run from backend/:
//   npx tsx evidence/rollcall/legiscan-ca-2172/worklist/lint.mjs <judgments.json>
// Prints WARN / BRITISH / LONG / SENTCOUNT lines; runbatch.py fails on WARN.
import { readFileSync } from "node:fs";
import { listPlainLanguageWarnings } from "../../../../src/pipeline/candidates/candidateRecordPlainLanguageLint.ts";
const d = JSON.parse(readFileSync(process.argv[2], "utf8"));
const T=[]; for (const j of d.judgments){T.push([j.measure_id+" "+j.roll+" yea",j.yea_description]);T.push([j.measure_id+" "+j.roll+" nay",j.nay_description]);}
let warn=0;
for (const [k,t] of T){const w=listPlainLanguageWarnings([{description:t}]); if(w.length){warn+=w.length;console.log("WARN",k,JSON.stringify(w));}}
const BRIT=/\b(centre|colour|labour|favour|honour|behaviour|analyse|analysed|recognise|recognised|authorise|authorised|penalise|penalised|utilise|utilised|organise|organised|programme|enrol|enrolment|fulfil|travelling|defence|licence|legalise|legalised|apologise)\b/i;
for (const [k,t] of T) if (BRIT.test(t)) console.log("BRITISH",k,t.match(BRIT)[0]);
function syl(w){w=w.toLowerCase().replace(/[^a-z]/g,"");if(!w)return 0;const m=w.match(/[aeiouy]+/g);let n=m?m.length:1;if(w.endsWith("e")&&n>1)n--;return Math.max(1,n);}
const MARK=String.fromCharCode(1);
const g=[];
for(const [k,t] of T){
  const masked=t.replace(/\b(?:v|U\.S|Rep|Sen|St|Mr|Mrs|Dr|No|[A-Z])\./g,(m)=>m.split(".").join(MARK));
  const sents=masked.split(/(?<=[.!?])\s+/).filter(Boolean);
  const words=t.split(/\s+/).filter(Boolean);
  const fk=0.39*(words.length/sents.length)+11.8*(words.reduce((a,w)=>a+syl(w),0)/words.length)-15.59;
  const long=sents.filter(s=>s.split(/\s+/).length>45);
  if(long.length) console.log("LONG",k,long.map(s=>s.split(/\s+/).length));
  if(sents.length<2||sents.length>4) console.log("SENTCOUNT",k,sents.length);
  g.push(+fk.toFixed(1));
}
g.sort((a,b)=>a-b);
console.log(`descriptions ${T.length} warnings ${warn} FKmedian ${g[Math.floor(g.length/2)]} worst ${g[g.length-1]}`);
