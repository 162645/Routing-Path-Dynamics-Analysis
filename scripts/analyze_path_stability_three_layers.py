import os,csv,json,time,getpass,statistics,re
from pathlib import Path
from collections import defaultdict,Counter
from datetime import datetime,timezone

for k in list(os.environ):
 if "proxy" in k.lower(): os.environ.pop(k)
import clickhouse_connect

root=Path(__file__).resolve().parents[1]/"outputs"
table=os.environ.get("CLICKHOUSE_TABLE","")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*",table):
 raise SystemExit("Set CLICKHOUSE_TABLE to database.table")
months=[
 ("2024-06","2024-06-22","2024-07-01"),
 ("2024-07","2024-07-01","2024-08-01"),
 ("2024-08","2024-08-01","2024-09-01"),
 ("2024-09","2024-09-01","2024-10-01"),
 ("2024-10","2024-10-01","2024-11-01"),
 ("2024-11","2024-11-01","2024-12-01"),
 ("2024-12","2024-12-01","2025-01-01"),
 ("2025-01","2025-01-01","2025-01-10"),
]
labels=[m[0] for m in months]
n=len(months)
full=list(range(1,7))
variants=["AS","ASGeo"]

pw=getpass.getpass("ClickHouse password: ")
client=clickhouse_connect.get_client(
 host=os.environ["CLICKHOUSE_HOST"],
 port=int(os.environ.get("CLICKHOUSE_PORT","8123")),
 username=os.environ["CLICKHOUSE_USER"],password=pw,
 database=os.environ.get("CLICKHOUSE_DATABASE","default"),connect_timeout=20,
 send_receive_timeout=1800)
client.query("SELECT 1")
pw=None

stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out=root/("path_stability_three_layers_"+stamp)
out.mkdir(parents=True,exist_ok=False)
figdir=out/"figures"
figdir.mkdir()
os.environ["MPLCONFIGDIR"]=str(out/"mplconfig")

route_data={}
prefix_ids={}
dominants={}
monthly_totals={(v,i):0 for v in variants for i in range(n)}
monthly_shares={(v,i):[] for v in variants for i in range(n)}
monthly_paths={(v,i):[] for v in variants for i in range(n)}
ties={(v,i):0 for v in variants for i in range(n)}
detail=open(out/"prefix24_monthly_main_route.csv",
            "w",encoding="utf-8",newline="")
dw=csv.writer(detail)
dw.writerow(["month","variant","prefix24","path_types",
 "observations","top_route","top_count","top_share","tie"])

t0=time.time()
for mi,(month,start,end) in enumerate(months):
 lo=start+" 00:00:00"
 hi=end+" 00:00:00"
 sql=(
  "SELECT prefix24,t.1,t.2,count() "
  "FROM "+table+" "
  "ARRAY JOIN [tuple('AS',as_path_text),"
  "tuple('ASGeo',asgeo_path_text)] AS t "
  "WHERE measure_time>=toDateTime64('"+lo+"',6,'UTC') "
  "AND measure_time<toDateTime64('"+hi+"',6,'UTC') "
  "AND t.2!='' GROUP BY prefix24,t.1,t.2"
 )
 pm=defaultdict(Counter)
 groups=0
 with client.query_row_block_stream(
  sql,settings={"max_threads":4,"max_execution_time":1800}
 ) as stream:
  for block in stream:
   for prefix,var,route,cnt in block:
    prefix=str(prefix); var=str(var); route=str(route)
    cnt=int(cnt)
    pm[(var,prefix)][route]+=cnt
    monthly_totals[(var,mi)]+=cnt
    if prefix not in prefix_ids:
     prefix_ids[prefix]=len(prefix_ids)
    pid=prefix_ids[prefix]
    key=(var,route)
    if key not in route_data:
     route_data[key]={
      "counts":[0]*n,"prefixes":[set() for _ in months]
     }
    route_data[key]["counts"][mi]+=cnt
    route_data[key]["prefixes"][mi].add(pid)
    groups+=1
 for (var,prefix),rc in pm.items():
  total=sum(rc.values())
  best=max(rc.values())
  tops=[r for r,cnt in rc.items() if cnt==best]
  tie=len(tops)>1
  top="" if tie else tops[0]
  share=best/total if total else 0
  monthly_shares[(var,mi)].append(share)
  monthly_paths[(var,mi)].append(len(rc))
  ties[(var,mi)]+=int(tie)
  state=dominants.setdefault((var,prefix),[None]*n)
  state[mi]="" if tie else top
  dw.writerow([month,var,prefix,len(rc),total,top,best,
               share,int(tie)])
 print("month_done",month,"groups",groups,
       "elapsed_s",round(time.time()-t0,1),flush=True)

detail.close()
client.close()

def quantile(vals,q):
 if not vals: return 0
 x=sorted(vals); z=(len(x)-1)*q
 a=int(z); b=min(a+1,len(x)-1); f=z-a
 return x[a]*(1-f)+x[b]*f

mrows=[]
for v in variants:
 for i,month in enumerate(labels):
  s=monthly_shares[(v,i)]
  p=monthly_paths[(v,i)]
  mrows.append({
   "month":month,"variant":v,"prefix24_count":len(s),
   "route_observations":monthly_totals[(v,i)],
   "median_top_share":statistics.median(s) if s else 0,
   "p10_top_share":quantile(s,.1),
   "p90_top_share":quantile(s,.9),
   "pct_top_share_ge_50":sum(x>=.5 for x in s)/len(s) if s else 0,
   "pct_top_share_ge_70":sum(x>=.7 for x in s)/len(s) if s else 0,
   "pct_top_share_ge_90":sum(x>=.9 for x in s)/len(s) if s else 0,
   "median_distinct_paths":statistics.median(p) if p else 0,
   "tied_top_pct":ties[(v,i)]/len(s) if s else 0
  })
with open(out/"prefix24_monthly_metrics.csv","w",newline="",
          encoding="utf-8") as f:
 w=csv.DictWriter(f,fieldnames=list(mrows[0]))
 w.writeheader(); w.writerows(mrows)

trans=[]
for v in variants:
 for i in range(n-1):
  denom=0; same=0
  for (vv,prefix),state in dominants.items():
   if vv!=v: continue
   a,b=state[i],state[i+1]
   if a and b:
    denom+=1; same+=int(a==b)
  trans.append({
   "variant":v,"from_month":labels[i],"to_month":labels[i+1],
   "unique_mode_both_months":denom,"same_modal_route":same,
   "retention":same/denom if denom else 0
  })
with open(out/"prefix24_modal_transitions.csv","w",newline="",
          encoding="utf-8") as f:
 w=csv.DictWriter(f,fieldnames=list(trans[0]))
 w.writeheader(); w.writerows(trans)

strict=[]
for v in variants:
 eligible=0; stable=0
 for (vv,prefix),state in dominants.items():
  if vv!=v: continue
  vals=[state[i] for i in full]
  if all(vals):
   eligible+=1; stable+=int(len(set(vals))==1)
 strict.append({
  "variant":v,"full_months":"2024-07..2024-12",
  "eligible_prefix24":eligible,"stable_prefix24":stable,
  "stable_share":stable/eligible if eligible else 0
 })
with open(out/"prefix24_six_month_stability.csv","w",
          newline="",encoding="utf-8") as f:
 w=csv.DictWriter(f,fieldnames=list(strict[0]))
 w.writeheader(); w.writerows(strict)

totobs={v:sum(monthly_totals[(v,i)] for i in range(n))
        for v in variants}
persist=[]; long=[]; life=defaultdict(lambda:[0,0])
cover=[]; jdetail=[]; alljac=defaultdict(list)
bothjac=defaultdict(list); heatmap=[]
for (v,route),info in route_data.items():
 counts=info["counts"]; sets=info["prefixes"]
 active=[i for i,x in enumerate(counts) if x]
 actfull=[i for i in full if counts[i]]
 obs=sum(counts)
 life[(v,len(active))][0]+=1
 life[(v,len(active))][1]+=obs
 cov=[len(x) for x in sets]
 persist.append({
  "variant":v,"route":route,"active_months_8":len(active),
  "active_complete_months_6":len(actfull),
  "months_present":";".join(labels[i] for i in active),
  "total_observations":obs,
  "overall_observation_share":obs/totobs[v] if totobs[v] else 0,
  "monthly_counts_json":json.dumps(counts),
  "monthly_prefix24_json":json.dumps(cov)
 })
 for i in active:
  row={
   "variant":v,"route":route,"month":labels[i],
   "traceroute_observations":counts[i],
   "observation_share":counts[i]/monthly_totals[(v,i)],
   "distinct_prefix24":cov[i]
  }
  long.append(row); cover.append(row)
 for i in range(n-1):
  a,b=sets[i],sets[i+1]
  union=len(a|b)
  if not union: continue
  inter=len(a&b); j=inter/union
  alljac[(v,i)].append(j)
  if a and b: bothjac[(v,i)].append(j)
  jdetail.append({
   "variant":v,"route":route,"from_month":labels[i],
   "to_month":labels[i+1],"intersection_prefix24":inter,
   "union_prefix24":union,"jaccard":j,
   "present_both":int(bool(a and b)),
   "full_month_pair":int(i in range(1,6))
  })
 heatmap.append((v,route,info))

def write_rows(name,rows):
 with open(out/name,"w",newline="",encoding="utf-8") as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]))
  w.writeheader(); w.writerows(rows)

write_rows("path_persistence.csv",persist)
write_rows("path_monthly_counts_and_coverage.csv",long)
write_rows("path_prefix_coverage.csv",cover)
write_rows("path_prefix_jaccard.csv",jdetail)
life_rows=[]
for v in variants:
 for k in range(1,n+1):
  nr,obs=life[(v,k)]
  life_rows.append({
   "variant":v,"active_months_8":k,"route_count":nr,
   "observations":obs,
   "observation_share":obs/totobs[v] if totobs[v] else 0
  })
write_rows("path_lifespan_distribution.csv",life_rows)

overlap=[]
for v in variants:
 for i in range(n-1):
  a=alljac[(v,i)]; b=bothjac[(v,i)]
  overlap.append({
   "variant":v,"from_month":labels[i],"to_month":labels[i+1],
   "routes_either":len(a),"routes_both":len(b),
   "median_jaccard_either":statistics.median(a) if a else 0,
   "median_jaccard_both":statistics.median(b) if b else 0,
   "p10_jaccard_both":quantile(b,.1),
   "p90_jaccard_both":quantile(b,.9),
   "full_month_pair":int(i in range(1,6))
  })
write_rows("path_prefix_overlap_summary.csv",overlap)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
plt.rcParams["font.sans-serif"]=["WenQuanYi Micro Hei",
                                 "DejaVu Sans"]
plt.rcParams["axes.spines.top"]=False
plt.rcParams["axes.spines.right"]=False

fig,axs=plt.subplots(3,1,figsize=(13,12))
for ax,v in zip(axs[:2],variants):
 data=[monthly_shares[(v,i)] for i in range(n)]
 ax.boxplot(data,showfliers=False,whis=(10,90))
 ax.axhline(.7,color="firebrick",ls="--",lw=1)
 ax.set_ylim(0,1.02); ax.set_ylabel("Top route share")
 ax.set_title(v+" route concentration by /24-month")
 ax.set_xticks(range(1,n+1)); ax.set_xticklabels(labels)
for v in variants:
 rr=[x for x in trans if x["variant"]==v]
 xx=[x["from_month"]+">"+x["to_month"] for x in rr]
 yy=[x["retention"] for x in rr]
 axs[2].plot(xx,yy,marker="o",label=v)
axs[2].set_ylim(0,1.02)
axs[2].set_ylabel("Modal route retention")
axs[2].set_title("Adjacent-month /24 dominant-route retention")
axs[2].legend(); axs[2].tick_params(axis="x",rotation=25)
fig.suptitle("Q1: /24 monthly route concentration and change")
fig.tight_layout()
fig.savefig(figdir/"01_prefix24_route_stability.png",
            dpi=180,bbox_inches="tight")
plt.close(fig)

fig,axs=plt.subplots(2,1,figsize=(12,9))
for ax,v in zip(axs,variants):
 ks=list(range(1,n+1))
 nr=[life[(v,k)][0] for k in ks]
 sh=[life[(v,k)][1]/totobs[v] for k in ks]
 ax.bar(ks,nr,color="#4477AA",alpha=.85)
 ax.set_ylabel("Distinct paths")
 ax.set_xlabel("Active months (of 8)")
 ax.set_title(v+" route recurrence and observation share")
 ax2=ax.twinx(); ax2.plot(ks,sh,color="#CC6677",marker="o")
 ax2.set_ylabel("Observation share"); ax.set_xticks(ks)
fig.tight_layout()
fig.savefig(figdir/"02_path_recurrence.png",
            dpi=180,bbox_inches="tight")
plt.close(fig)

fig,axs=plt.subplots(2,1,figsize=(14,12))
legend=[]
for ax,v in zip(axs,variants):
 arr=[(r,inf) for vv,r,inf in heatmap if vv==v]
 arr.sort(key=lambda x:sum(len(s) for s in x[1]["prefixes"]),
          reverse=True)
 top=arr[:12]
 mat=np.array([[len(inf["prefixes"][i]) for i in range(n)]
               for r,inf in top])
 names=[]
 for rank,(route,inf) in enumerate(top,1):
  names.append(route[:45]+"..." if len(route)>48 else route)
  legend.append({"variant":v,"rank":rank,"route":route})
 im=ax.imshow(np.log1p(mat),aspect="auto",cmap="viridis")
 ax.set_yticks(range(len(names))); ax.set_yticklabels(names,fontsize=7)
 ax.set_xticks(range(n)); ax.set_xticklabels(labels)
 ax.set_title(v+": top paths by total /24-month coverage")
 fig.colorbar(im,ax=ax,label="log(1 + distinct /24)")
fig.tight_layout()
fig.savefig(figdir/"03_path_prefix_coverage.png",
            dpi=180,bbox_inches="tight")
plt.close(fig)
write_rows("top_path_heatmap_legend.csv",legend)

fig,axs=plt.subplots(2,1,figsize=(13,9))
for ax,v in zip(axs,variants):
 rr=[x for x in overlap if x["variant"]==v]
 xx=[x["from_month"]+">"+x["to_month"] for x in rr]
 aa=[x["median_jaccard_either"] for x in rr]
 bb=[x["median_jaccard_both"] for x in rr]
 ax.plot(xx,aa,marker="o",label="Present either month")
 ax.plot(xx,bb,marker="s",label="Present both months")
 ax.set_ylim(0,1.02); ax.set_ylabel("Median Jaccard")
 ax.set_title(v+" route /24-set overlap"); ax.legend()
 ax.tick_params(axis="x",rotation=25)
fig.tight_layout()
fig.savefig(figdir/"04_path_prefix_overlap.png",
            dpi=180,bbox_inches="tight")
plt.close(fig)

summary={
 "source":table,
 "read_only":True,"ping_used":False,
 "window_utc":"[2024-06-22,2025-01-10)",
 "months":labels,
 "partial_months":["2024-06","2025-01"],
 "monthly_observations":{
  v:[monthly_totals[(v,i)] for i in range(n)] for v in variants
 },
 "monthly_prefix24":{
  v:[len(monthly_shares[(v,i)]) for i in range(n)]
  for v in variants
 },
 "distinct_paths":{
  v:sum(1 for vv,r in route_data if vv==v) for v in variants
 },
 "strict_six_month_prefix_stability":strict,
 "output_directory":str(out)
}
with open(out/"summary.json","w",encoding="utf-8") as f:
 json.dump(summary,f,ensure_ascii=False,indent=2)
(out/"README.md").write_text(
 "Three-layer path stability analysis\n"
 "Source: ClickHouse; read-only; no Ping. "
 "UTC [2024-06-22, 2025-01-10). "
 "June and January are partial months.\n"
 "Q1: top route share per /24-month and adjacent modal-route "
 "retention. Strict six-month stability uses July-December.\n"
 "Q2: exact path strings; monthly observations/shares and "
 "active months.\n"
 "Q3: distinct /24 count per path-month; adjacent Jaccard is "
 "intersection/union of the path's /24 sets. "
 "'Either' includes zeros for absent routes; 'both' conditions "
 "on presence in both months.\n"
 "Rows are sampled traceroute observations, not all addresses. "
 "ASGeo inherits mapping-quality limits.\n",encoding="utf-8")
print("analysis_complete",str(out),flush=True)
print("prefix24_rows",sum(len(monthly_shares[(v,i)])
                          for v in variants for i in range(n)),
      flush=True)
print("path_counts",summary["distinct_paths"],flush=True)
print("six_month_stability",strict,flush=True)
print("figures",sorted(x.name for x in figdir.glob("*.png")),
      flush=True)

