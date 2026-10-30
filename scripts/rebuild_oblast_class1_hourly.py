#!/usr/bin/env python3
"""Rebuild hourly five-method ASGeo status figures for all Odesa Class-I routes.

Reads frozen project state outputs and Chapter 3 membership evidence; writes only
to a new, versioned subdirectory. Original data and figures are not modified.
"""
from __future__ import annotations
import json, math, textwrap, hashlib
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.patches import Patch
from matplotlib.backends.backend_pdf import PdfPages

ROOT = Path('/home/wsl/zhongqibaogao/asgeo_quality_sampling_research_20261029')
BASE = Path('/home/wsl/zhongqibaogao/ch3_ch4_D_final_linkage_20261027')
CH3 = Path('/home/wsl/zhongqibaogao/chapter3_path_identity_vclass_closure_20261020')
OUT = ROOT / 'results/oblast_classI_hourly_20261030'
FIG = ROOT / 'figures/oblast_classI_hourly_20261030_a4_final'
PRIV = ROOT / 'private/oblast_classI_hourly_20261030'
REPORT = ROOT / 'reports/oblast_classI_hourly_20261030_a4_final.md'
MEMBERS = BASE / 'results/chapter4_45_46_D_final/private/path_membership_private.csv'
REGIONS = BASE / 'source_bundle/data/version_audit/chapter4_evidence/ping_core_B_D_region_aggregates_private.csv.gz'
CLASSES = CH3 / 'private_server_only/prefix24_final_five_class_private.csv'
MAP = ROOT / 'private/three_state_revision_20261030/path_identity_mapping_private.csv'
STATES = ROOT / 'results/three_state_revision_20261030/path_cycle_method_states.parquet'
SCOPE = ROOT / 'results/data_scope_audit.json'
THRESHOLDS = ROOT / 'results/three_state_revision_20261030/five_methods_threshold_audit.csv'
COLORS = ['#DFEAF2', '#B63B3B', '#B4B4B4']
CMAP = ListedColormap(COLORS, name='three_discrete_states')
NORM = BoundaryNorm([-0.5, 0.5, 1.5, 2.5], CMAP.N)
METHODS = ['m1_blameit_adapt','m2_asgeo_quantile','m3_response_drop','m4_active24','m5_ch4_path']
NAMES = {
 'm1_blameit_adapt':'(a) BlameIt-inspired RTT 尾部阈值适配',
 'm2_asgeo_quantile':'(b) ASGeo 路径 RTT 分位超阈代理',
 'm3_response_drop':'(c) 路径响应 IP 数下尾检测',
 'm4_active24':'(d) IODA 思路适配：活跃 /24 计数',
 'm5_ch4_path':'(e) 第四章正式法：IPS + RTT 路径关联'}
UTC = ZoneInfo('UTC')
KYIV = ZoneInfo('Europe/Kyiv')

def md_table(df):
    if df.empty: return '(无记录)'
    cols=list(df.columns); lines=['| '+' | '.join(map(str,cols))+' |','| '+' | '.join(['---']*len(cols))+' |']
    for row in df.itertuples(index=False,name=None):
        lines.append('| '+' | '.join(str(v).replace('|','\\|').replace('\n',' ') for v in row)+' |')
    return '\n'.join(lines)

def configure_fonts():
    from matplotlib import font_manager
    names={f.name for f in font_manager.fontManager.ttflist}
    font=next((x for x in ['Noto Sans CJK SC','WenQuanYi Zen Hei','WenQuanYi Micro Hei','Arial Unicode MS'] if x in names),'DejaVu Sans')
    plt.rcParams.update({'font.family':'sans-serif','font.sans-serif':[font,'DejaVu Sans'],
      'axes.unicode_minus':False,'svg.fonttype':'none','pdf.fonttype':42,'ps.fonttype':42,
      'figure.facecolor':'white','axes.facecolor':'white'})
    return font

def load_data():
    for p in [MEMBERS,REGIONS,CLASSES,MAP,STATES,SCOPE,THRESHOLDS]:
        if not p.exists(): raise FileNotFoundError(p)
    mem=pd.read_csv(MEMBERS,dtype={'prefix24':str,'path_id':str})
    cls=pd.read_csv(CLASSES,dtype={'prefix24':str})[['prefix24','class_label','strict_evidence_months','missing_evidence_months','qualified_main_route_months','distinct_qualified_routes','strict_stable_route']]
    mp=pd.read_csv(MAP,dtype={'path_id':str})
    rg=pd.read_csv(REGIONS,dtype={'prefix24':str})
    rg['region_observations']=pd.to_numeric(rg.region_observations,errors='coerce').fillna(0)
    rg=rg.sort_values(['prefix24','region_observations','region'],ascending=[True,False,True])
    total=rg.groupby('prefix24').region_observations.transform('sum')
    rg['region_share']=rg.region_observations/total.where(total>0)
    modal=rg.drop_duplicates('prefix24').rename(columns={'region':'modal_region','region_observations':'modal_region_observations'})[['prefix24','modal_region','modal_region_observations','region_share']]
    mem=mem.merge(cls,on='prefix24',how='left',validate='one_to_one').merge(modal,on='prefix24',how='left',validate='one_to_one')
    if mem.class_label.isna().any() or mem.modal_region.isna().any(): raise AssertionError('Unmatched class or Ping-region evidence')
    mem=mem.merge(mp[['path_id','path_alias','stable_route','ch3_class_status','association_window_start_utc','association_window_end_exclusive_utc']],on='path_id',how='left',validate='many_to_one',suffixes=('','_mapping'))
    if mem.path_alias.isna().any(): raise AssertionError('A formal path member has no identity mapping')
    mem['is_class_i']=mem.class_label.eq('Ⅰ类')
    mem['purity_eligible']=mem.region_share.ge(.80)
    path=mem.groupby('path_id',as_index=False).agg(path_alias=('path_alias','first'),stable_route=('stable_route','first'),
      prefix24_n=('prefix24','nunique'),all_members_class_i=('is_class_i','all'),all_members_purity_ge_080=('purity_eligible','all'),
      modal_region_n=('modal_region','nunique'),oblast=('modal_region','first'),min_region_purity=('region_share','min'),mean_region_purity=('region_share','mean'),
      ch3_class_status=('ch3_class_status','first'),class_i_member_n=('is_class_i','sum'),
      evidence_months_min=('strict_evidence_months','min'),evidence_months_max=('strict_evidence_months','max'),
      missing_evidence_months_max=('missing_evidence_months','max'),qualified_route_months_min=('qualified_main_route_months','min'),
      distinct_routes_max=('distinct_qualified_routes','max'))
    path=path[(path.all_members_class_i)&(path.all_members_purity_ge_080)&(path.modal_region_n.eq(1))].copy()
    path['path_order']=path.path_id.str.extract(r'(\d+)').astype(int)
    path=path.sort_values(['path_order','path_id']).reset_index(drop=True)
    path['full_route_label']=path.path_alias+' | '+path.stable_route
    mem=mem.merge(path[['path_id','path_order','oblast']],on='path_id',how='inner',suffixes=('','_path'))
    states=pd.read_parquet(STATES)
    states['timestamp_utc']=pd.to_datetime(states.timestamp_utc,utc=True)
    states['state_code']=pd.to_numeric(states.state,errors='coerce').map({0:0,1:1}).fillna(2).astype('int8')
    states['path_order']=states.path_id.str.extract(r'(\d+)').astype(int)
    scope=json.loads(SCOPE.read_text())
    return mem,path,states,scope

def region_audit(mem,path,states):
    # Count paths only if every member has one >=0.80 modal region and all members are Class I.
    eligible=mem[mem.class_label.eq('Ⅰ类') & mem.region_share.ge(.80)].copy()
    good=eligible.groupby('path_id').agg(member_n=('prefix24','nunique'),region_n=('modal_region','nunique'),region=('modal_region','first'),all_i=('class_label',lambda s:s.eq('Ⅰ类').all()))
    good=good[(good.region_n.eq(1))&good.all_i].reset_index()
    all_i_path=mem.groupby('path_id').agg(all_i=('class_label',lambda s:s.eq('Ⅰ类').all()),member_n=('prefix24','nunique'))
    all_i_ids=all_i_path[all_i_path.all_i].index
    good=good.merge(all_i_path[['member_n']],left_on='path_id',right_index=True,how='left',suffixes=('','_expected'))
    good=good[good.member_n.eq(good.member_n_expected)]
    base=good[good.path_id.isin(all_i_ids)]
    # Region spelling is retained as provided by frozen Ping-region source.
    result=[]
    local_start=pd.Timestamp('2024-11-16 22:00:00Z'); local_end=pd.Timestamp('2024-11-17 22:00:00Z')
    for reg,g in base.groupby('region',sort=True):
        ids=set(g.path_id)
        ss=states[states.path_id.isin(ids)]
        ev=ss[(ss.timestamp_utc>=local_start)&(ss.timestamp_utc<local_end)]
        cover=ss.groupby('method').state_code.apply(lambda x:float((x!=2).mean())).to_dict()
        evc=ev.groupby(['method','state_code']).size().to_dict()
        result.append({'region_source_name':reg,'class_i_path_n':len(ids),'associated_prefix24_n':int(g.member_n.sum()),
          'minimum_member_region_purity':float(mem[mem.path_id.isin(ids)].region_share.min()),
          'mean_method_valid_coverage':float(np.mean(list(cover.values()))) if cover else np.nan,
          'm1_valid_coverage':cover.get(METHODS[0],np.nan),'m2_valid_coverage':cover.get(METHODS[1],np.nan),
          'm3_valid_coverage':cover.get(METHODS[2],np.nan),'m4_valid_coverage':cover.get(METHODS[3],np.nan),'m5_valid_coverage':cover.get(METHODS[4],np.nan),
          'nov17_local_m1_normal':evc.get((METHODS[0],0),0),'nov17_local_m1_anomaly':evc.get((METHODS[0],1),0),'nov17_local_m1_unknown':evc.get((METHODS[0],2),0),
          'nov17_local_m2_normal':evc.get((METHODS[1],0),0),'nov17_local_m2_anomaly':evc.get((METHODS[1],1),0),'nov17_local_m2_unknown':evc.get((METHODS[1],2),0),
          'nov17_local_m3_normal':evc.get((METHODS[2],0),0),'nov17_local_m3_anomaly':evc.get((METHODS[2],1),0),'nov17_local_m3_unknown':evc.get((METHODS[2],2),0),
          'nov17_local_m4_normal':evc.get((METHODS[3],0),0),'nov17_local_m4_anomaly':evc.get((METHODS[3],1),0),'nov17_local_m4_unknown':evc.get((METHODS[3],2),0),
          'nov17_local_m5_normal':evc.get((METHODS[4],0),0),'nov17_local_m5_anomaly':evc.get((METHODS[4],1),0),'nov17_local_m5_unknown':evc.get((METHODS[4],2),0)})
    return pd.DataFrame(result).sort_values(['class_i_path_n','associated_prefix24_n','region_source_name'],ascending=[False,False,True])

def event_registry():
    rows=[]
    for eid,name,d in [('nov17','2024-11-17 乌克兰大规模袭击（日期级定位）','2024-11-17'),('nov28','2024-11-28 乌克兰全国能源设施袭击（敖德萨相关性未确认）','2024-11-28')]:
        local0=pd.Timestamp(d+' 00:00').tz_localize(KYIV)
        local1=(pd.Timestamp(d+' 00:00')+pd.Timedelta(days=1)).tz_localize(KYIV)
        rows.append({'event_id':eid,'event_name':name,'local_start_date_only':local0.isoformat(),'local_end_exclusive_date_only':local1.isoformat(),
          'utc_start_for_date_alignment_only':local0.tz_convert('UTC').isoformat(),'utc_end_exclusive_for_date_alignment_only':local1.tz_convert('UTC').isoformat(),
          'timezone':'Europe/Kyiv -> UTC','temporal_precision':'date only; not an onset/end estimate',
          'nov17_local_odesa_impact_source':'President of Ukraine, 17 Nov 2024 address; identifies Odesa region',
          'nov17_source':'https://www.president.gov.ua/en/news/golub-miru-nadislav-nam-chergovi-kindzhali-ta-kalibri-os-tak-94461',
          'nov28_source':'https://ukraine.un.org/en/284745-plight-civilians-result-attacks-energy-network',
          'nov28_local_odesa_impact':'not confirmed by this source; national event date only'})
    return pd.DataFrame(rows)

def time_audit(states):
    rows=[]
    for m,g in states.groupby('method',sort=False):
        known=g[g.state_code.ne(2)]
        reasons=g[g.state_code.eq(2)].groupby('unknown_reason',dropna=False).size().sort_values(ascending=False)
        rows.append({'method':m,'formal_eval_start_utc':g.timestamp_utc.min().isoformat(),'formal_eval_end_utc':g.timestamp_utc.max().isoformat(),
          'first_path_hour_decision_utc':known.timestamp_utc.min().isoformat() if len(known) else '',
          'last_path_hour_decision_utc':known.timestamp_utc.max().isoformat() if len(known) else '',
          'known_path_hour_n':len(known),'unknown_path_hour_n':len(g)-len(known),
          'unknown_reasons_top':json.dumps({str(k):int(v) for k,v in reasons.head(10).items()},ensure_ascii=False)})
    return pd.DataFrame(rows)

def unknown_interval_audit(states):
    u=states[states.state_code.eq(2)].sort_values(['path_id','method','timestamp_utc']).copy()
    columns=['path_id','path_alias','method','unknown_reason','start_utc','end_sample_utc','unknown_hours']
    if u.empty:
        intervals=pd.DataFrame(columns=columns)
    else:
        keys=['path_id','method']
        gap=u.groupby(keys).timestamp_utc.diff().ne(pd.Timedelta(hours=1))
        reason_change=u.groupby(keys).unknown_reason.shift().ne(u.unknown_reason)
        u['_new_interval']=gap|reason_change
        u['_interval_id']=u.groupby(keys)['_new_interval'].cumsum()
        intervals=(u.groupby(['path_id','path_alias','method','unknown_reason','_interval_id'],dropna=False,as_index=False)
          .agg(start_utc=('timestamp_utc','min'),end_sample_utc=('timestamp_utc','max'),unknown_hours=('timestamp_utc','size'))
          .drop(columns='_interval_id'))
    history=intervals[intervals.unknown_reason.astype('string').str.contains('history|historical|prior|baseline',case=False,regex=True,na=False)].copy()
    if history.empty:
        history=pd.DataFrame([{'path_id':'','path_alias':'','method':m,'unknown_reason':'No history-insufficiency reason in formal state outputs',
          'start_utc':'','end_sample_utc':'','unknown_hours':0} for m in METHODS],columns=columns)
    return intervals,history

def run_audit(states):
    z=states.sort_values(['path_id','method','timestamp_utc']).copy()
    z['break']=z.groupby(['path_id','method']).timestamp_utc.diff().ne(pd.Timedelta(hours=1)) | z.groupby(['path_id','method']).state_code.shift().ne(1)
    z['run_id']=z.groupby(['path_id','method'])['break'].cumsum()
    runs=z[z.state_code.eq(1)].groupby(['path_id','path_alias','path_order','method','run_id'],as_index=False).agg(hours=('timestamp_utc','size'),start_utc=('timestamp_utc','min'),end_sample_utc=('timestamp_utc','max'))
    return runs.sort_values(['hours','start_utc'],ascending=[False,True]).reset_index(drop=True)

def plot_page(st, paths, window_id, window_title, start, hours, page, total_pages, pdf):
    plot=st[(st.timestamp_utc>=start)&(st.timestamp_utc<start+pd.Timedelta(hours=hours))&st.path_id.isin(paths.path_id)]
    expected=pd.date_range(start=start,periods=hours,freq='h',tz='UTC')
    # Ensure every scheduled period is present for each displayed path and method; absent result is NA with explicit reason.
    idx=pd.MultiIndex.from_product([paths.path_id,expected,METHODS],names=['path_id','timestamp_utc','method'])
    plot=plot.set_index(['path_id','timestamp_utc','method'])[['state_code','unknown_reason']].reindex(idx).reset_index()
    absent=plot.state_code.isna()
    plot.loc[absent,'state_code']=2
    plot.loc[absent,'unknown_reason']='missing_result_row_in_formal_hourly_grid'
    plot['state_code']=plot.state_code.astype(int)
    # common matrix positions; chronological route order is fixed across all five panels/pages.
    fig,axes=plt.subplots(5,1,figsize=(11.69,8.27),sharex=True,sharey=True,gridspec_kw={'left':.52,'right':.99,'top':.90,'bottom':.22,'hspace':.27})
    n=len(paths)
    x=np.arange(hours+1)-.5
    y=np.arange(n+1)-.5
    for j,(ax,m) in enumerate(zip(axes,METHODS)):
        sub=plot[plot.method.eq(m)].pivot(index='path_id',columns='timestamp_utc',values='state_code').reindex(index=paths.path_id,columns=expected)
        arr=sub.to_numpy(dtype=int)
        ax.pcolormesh(x,y,arr,cmap=CMAP,norm=NORM,shading='flat',rasterized=True,linewidth=0)
        ax.set_ylim(n-.5,-.5)
        ax.set_yticks(np.arange(n))
        labels=[textwrap.fill(str(v),width=58,break_long_words=False,break_on_hyphens=False) for v in paths.full_route_label]
        ax.set_yticklabels(labels,fontsize=7.0,linespacing=1.05)
        ax.tick_params(axis='y',length=0,pad=5)
        ax.set_title(NAMES[m],loc='left',fontsize=9.2,pad=3,fontweight='medium')
        ax.set_xlim(-.5,hours-.5)
        ax.set_xticks(np.arange(0,hours,3 if hours>=60 else 1))
        ax.set_xticklabels([expected[i].strftime('%m-%d %H:%M') for i in np.arange(0,hours,3 if hours>=60 else 1)],rotation=90,ha='center',fontsize=6.4)
        ax.tick_params(axis='x',length=2,pad=2)
        for t in ax.spines.values(): t.set_color('#AEB7BC'); t.set_linewidth(.45)
        ax.set_yticks(np.arange(-.5,n,1),minor=True); ax.grid(which='minor',axis='y',color='white',linewidth=.5); ax.tick_params(which='minor',left=False)
        # Exact external date locator; the grey band indicates date bounds, not impact duration.
        show_nov17 = window_id=='event_nov17_72h'
        nov28_start=pd.Timestamp('2024-11-27 22:00Z'); nov28_end=pd.Timestamp('2024-11-28 22:00Z')
        show_nov28 = window_id.startswith('longest_') and start < nov28_end and start+pd.Timedelta(hours=hours) > nov28_start
        if show_nov17 or show_nov28:
            a=pd.Timestamp('2024-11-16 22:00Z') if show_nov17 else nov28_start
            b=pd.Timestamp('2024-11-17 22:00Z') if show_nov17 else nov28_end
            ax.axvspan((a-start)/pd.Timedelta(hours=1)-.5,(b-start)/pd.Timedelta(hours=1)-.5,color='#777777',alpha=.045,zorder=2)
            ax.axvline((a-start)/pd.Timedelta(hours=1)-.5,color='#666666',ls='--',lw=.7,zorder=3)
            ax.axvline((b-start)/pd.Timedelta(hours=1)-.5,color='#666666',ls='--',lw=.7,zorder=3)
    axes[-1].set_xlabel('UTC 时间；每格为一个连续小时测量周期（数据样本时间戳为每小时 :28；所有小时单元均保留）',fontsize=8.2,labelpad=5)
    handles=[Patch(facecolor=COLORS[0],edgecolor='#74828B',label='正常（0）'),Patch(facecolor=COLORS[1],edgecolor='#8D3030',label='异常（1）'),Patch(facecolor=COLORS[2],edgecolor='#777777',label='无法判定（NA）')]
    fig.legend(handles=handles,loc='upper right',bbox_to_anchor=(.98,.965),ncol=3,frameon=False,fontsize=8.3)
    subtitle=f'敖德萨州 | Ⅰ类稳定 ASGeo 路径 | UTC | {expected[0]:%Y-%m-%d %H:%M} — {expected[-1]:%Y-%m-%d %H:%M} | 第 {page}/{total_pages} 页；完整路径仅按固定 path_order 分页'
    fig.suptitle(window_title,fontsize=14,fontweight='semibold',x=.49,y=.975,ha='center')
    fig.text(.49,.936,subtitle,ha='center',va='top',fontsize=8.1,color='#354047')
    if window_id=='event_nov17_72h': note='灰色带仅定位 Kyiv 当地 2024-11-17 日期（换算 UTC），外部资料无精确攻击起止时刻；不表示事件持续时间或因果关系。'
    elif show_nov28: note='灰色带仅标记 2024-11-28 全国事件日期；现有来源未确认敖德萨州受影响。该标记与检测到的连续异常时段彼此独立。'
    else: note='灰色状态为方法证据不足或缺少正式结果；并非正常。事件之外的窗口作为状态对照，不据此断言当日绝无其他事件。'
    fig.text(.49,.035,note,ha='center',va='bottom',fontsize=6.8,color='#40494E',wrap=True)
    filename=f'{window_id}_p{page:02d}'
    for ext in ['svg','png']:
        path=FIG/f'{filename}.{ext}'
        fig.savefig(path,dpi=400 if ext=='png' else None,bbox_inches=None)
    pdf.savefig(fig)
    plt.close(fig)
    return {'figure_page':filename,'path_ids':paths.path_id.tolist(),'hour_cells':hours,'first_sample_utc':expected[0].isoformat(),'last_sample_utc':expected[-1].isoformat(),'row_label_count':n}

def render_window(states, paths, wid, title, start, hours):
    pages=[]; chunks=[paths.iloc[i:i+2].copy() for i in range(0,len(paths),2)]
    pdf_path=FIG/f'{wid}.pdf'
    with PdfPages(pdf_path,metadata={'Title':title,'Subject':'三态离散逐小时状态矩阵','Creator':'rebuild_oblast_class1_hourly.py'}) as pdf:
        for i,g in enumerate(chunks,1): pages.append(plot_page(states,g,wid,title,start,hours,i,len(chunks),pdf))
    return pages

def main():
    for d in [OUT,FIG,PRIV,REPORT.parent]: d.mkdir(parents=True,exist_ok=True)
    font=configure_fonts()
    mem,allpaths,allstates,scope=load_data()
    regiontab=region_audit(mem,allpaths,allstates)
    # Event relevance is determined by an authoritative source naming Odesa; do not rank on anomaly count.
    mask=allpaths.oblast.eq('敖德萨州')
    paths=allpaths[mask].sort_values(['path_order','path_id']).reset_index(drop=True)
    if len(paths)!=21 or int(paths.prefix24_n.sum())!=47: raise AssertionError(f'Frozen Odesa Class-I cohort changed: {len(paths)} paths, {paths.prefix24_n.sum()} prefixes')
    ids=set(paths.path_id)
    states=allstates[allstates.path_id.isin(ids)].copy().sort_values(['path_order','timestamp_utc','method']).reset_index(drop=True)
    # Keep a complete private member crosswalk; the diagram uses full ordered ASGeo sequences verbatim.
    selected_members=mem[mem.path_id.isin(ids)].copy().sort_values(['path_order','prefix24'])
    selected_members.to_csv(PRIV/'odesa_classI_full_member_crosswalk.csv',index=False,encoding='utf-8-sig')
    paths.to_csv(PRIV/'odesa_classI_path_crosswalk.csv',index=False,encoding='utf-8-sig')
    regiontab.to_csv(OUT/'oblast_classI_selection_audit.csv',index=False,encoding='utf-8-sig')
    states.to_csv(OUT/'odesa_classI_hourly_states_all_formal_cycles.csv.gz',index=False,encoding='utf-8-sig',compression='gzip')
    # Exact unknown-cause accounting, by method, for whole region evaluation and plotted windows.
    unk=states[states.state_code.eq(2)].groupby(['method','unknown_reason'],dropna=False).size().rename('unknown_path_hour_n').reset_index()
    unk.to_csv(OUT/'unknown_reason_counts.csv',index=False,encoding='utf-8-sig')
    unknown_intervals,history_intervals=unknown_interval_audit(states)
    unknown_intervals.to_csv(OUT/'unknown_intervals.csv',index=False,encoding='utf-8-sig')
    history_intervals.to_csv(OUT/'history_insufficiency_intervals.csv',index=False,encoding='utf-8-sig')
    time_audit(states).to_csv(OUT/'time_range_method_audit.csv',index=False,encoding='utf-8-sig')
    pd.read_csv(THRESHOLDS).to_csv(OUT/'five_methods_threshold_and_source_audit.csv',index=False,encoding='utf-8-sig')
    ev=event_registry(); ev.to_csv(OUT/'event_utc_dst_audit.csv',index=False,encoding='utf-8-sig')
    runs=run_audit(states); runs.to_csv(OUT/'continuous_anomaly_runs_all.csv',index=False,encoding='utf-8-sig')
    if runs.empty: raise AssertionError('No real anomaly run found in the selected Class-I region')
    best=runs.iloc[0]
    # Window positions follow the actual sample schedule (:28 UTC); each interval retains every consecutive hour.
    event_start=pd.Timestamp('2024-11-15 22:28:00Z'); event_hours=72
    control_start=pd.Timestamp('2024-11-08 22:28:00Z'); control_hours=72
    long_start=pd.Timestamp(best.start_utc); long_len=int(best.hours)
    pages=[]
    pages+=render_window(states,paths,'event_nov17_72h','乌克兰重大事件日期前后 72 小时逐小时对比（日期级标注）',event_start,event_hours)
    pages+=render_window(states,paths,'control_nov10_72h','正常时期匹配对照：当地 2024-11-10 日期前后 72 小时逐小时对比',control_start,control_hours)
    # Split the longest observed uninterrupted method-positive run into consecutive <=72-hour pieces.
    part=0; offset=0
    while offset<long_len:
        take=min(72,long_len-offset); wid=f'longest_{best.method}_{best.path_alias}_{long_start:%m%d%H}_part{part+1}'
        pages+=render_window(states,paths,wid,f'真实最长连续异常展开：{best.path_alias} / {best.method} | 第 {part+1} 段（{take} 个连续小时）',long_start+pd.Timedelta(hours=offset),take)
        offset+=take; part+=1
    pd.DataFrame(pages).to_csv(OUT/'figure_page_manifest.csv',index=False,encoding='utf-8-sig')
    # State distribution summaries for identical route-hour evaluation domains.
    dist=states.groupby(['method','state_code']).size().unstack(fill_value=0).reindex(columns=[0,1,2],fill_value=0)
    dist['all_path_hour_n']=dist.sum(axis=1)
    dist['normal_pct']=100*dist[0]/dist.all_path_hour_n; dist['anomaly_pct']=100*dist[1]/dist.all_path_hour_n; dist['unknown_pct']=100*dist[2]/dist.all_path_hour_n
    dist=dist.rename(columns={0:'normal_n',1:'anomaly_n',2:'unknown_n'}).reset_index()
    dist.to_csv(OUT/'method_state_distribution_full_eval.csv',index=False,encoding='utf-8-sig')
    # Compare methods on exact path-hour cells where every method is determinate.
    # This shared-evidence denominator supplements, rather than replaces, NA reporting.
    common_wide=states.pivot(index=['path_id','timestamp_utc'],columns='method',values='state_code').reindex(columns=METHODS)
    common_mask=common_wide.notna().all(axis=1) & common_wide.ne(2).all(axis=1)
    common_counts=[]
    for method in METHODS:
        vals=common_wide.loc[common_mask,method].astype(int)
        common_counts.append({'method':method,'common_known_path_hour_n':int(common_mask.sum()),
          'common_coverage_pct_of_method_eval':100*int(common_mask.sum())/int((states.method==method).sum()),
          'normal_n':int(vals.eq(0).sum()),'anomaly_n':int(vals.eq(1).sum()),
          'normal_pct_common':100*float(vals.eq(0).mean()),'anomaly_pct_common':100*float(vals.eq(1).mean())})
    common_dist=pd.DataFrame(common_counts)
    common_dist.to_csv(OUT/'method_state_distribution_common_eval.csv',index=False,encoding='utf-8-sig')
    # Per-window event/control counts; external date period is exactly the Kyiv local date converted to UTC.
    evs=[]
    windows=[('event_local_date','2024-11-16 22:28Z','2024-11-17 21:28Z'),('control_local_date','2024-11-09 22:28Z','2024-11-10 21:28Z')]
    for label,a,b in windows:
        a=pd.Timestamp(a); b=pd.Timestamp(b); q=states[states.timestamp_utc.between(a,b,inclusive='both')]
        for m,g in q.groupby('method'):
            c=g.state_code.value_counts().to_dict(); evs.append({'period':label,'method':m,'normal_n':c.get(0,0),'anomaly_n':c.get(1,0),'unknown_n':c.get(2,0),'hourly_path_cells':len(g),'external_event_claim':'date locator only; no duration/cause'})
    pd.DataFrame(evs).to_csv(OUT/'event_control_status_counts.csv',index=False,encoding='utf-8-sig')
    # Provenance hashes and complete file manifest (no credentials or raw DB data).
    inputs=[MEMBERS,REGIONS,CLASSES,MAP,STATES,THRESHOLDS,SCOPE]
    manifest={'generated_utc':pd.Timestamp.now(tz='UTC').isoformat(),'font':font,'region':'敖德萨州','path_n':len(paths),'prefix24_n':int(paths.prefix24_n.sum()),
      'timezone':'UTC','cycle_granularity':'hourly; nominal timestamp :28 UTC','state_mapping':{'0':'normal','1':'formal method anomaly flag','NA':'unknown; preserved as neutral grey'},
      'full_route_identity':'exact stable_route text; no node omission/ellipsis','png_dpi':400,'page_height_rule':'up to 2 paths per A4 landscape page in fixed path_order; every method shares same paths/time bins',
      'longest_observed_run':{'path_id':str(best.path_id),'method':str(best.method),'hours':int(best.hours),'start_utc':best.start_utc.isoformat(),'end_sample_utc':best.end_sample_utc.isoformat()},
      'input_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}}
    (OUT/'generation_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    # Compact outcome report with region choice rationale and explicit limits.
    threshold=pd.read_csv(THRESHOLDS)
    tdisplay=threshold[['method','method_name','source','history','threshold','current_min','history_min','unknown','source_code','original_alignment']].copy()
    scope_text=json.dumps(scope,ensure_ascii=False,indent=2)
    report=f'''# 敖德萨州Ⅰ类 ASGeo 路径五方法逐小时三态对比整改\n\n生成时间：{manifest['generated_utc']}。所有时间轴统一 UTC；Matplotlib 字体：{font}。\n\n## 区域选择与研究对象\n\n区域选择先看外部事件资料的明确地理指向，再在满足第三章正式路径条件、路径成员全部属于Ⅰ类、所有成员 Ping 主地区纯度至少 0.80 且整条路径成员都落入同一地区的候选路径中统计规模与覆盖，不使用第四章异常数挑区。总统府 2024-11-17 地址明确提到敖德萨州；候选路径区域表见 `oblast_classI_selection_audit.csv`。敖德萨州有 {len(paths)} 条合格Ⅰ类 ASGeo 稳定路径、{int(paths.prefix24_n.sum())} 个原始 `/24` 成员，成员最低主地区纯度 {paths.min_region_purity.min():.3f}。选择依据不是异常较多。\n\n纳入规则：第三章冻结五分类的每一个成员 `/24` 均标为Ⅰ类；第四章 D-core `reached_target=1` 严格到达过滤路径成员；Ping 成员主地区份额均 ≥0.80；同一路径的成员主地区一致。图与路径对照表保留全部成员；图只描述对应州内全部合格路径，不将途经 AS 节点解释成末端州属。该Ⅰ类案例图不能外推到Ⅱ—Ⅴ类。\n\n## 时间与历史窗口审计\n\nClickHouse 原始 Ping 范围（先前只读核验）：2024-06-22 08:28:49 UTC 至 2025-01-09 14:54:10 UTC；服务端字段为 `DateTime64(6, UTC)`。正式 D-core 为 2024-07-01 00:28 至 2024-12-31 23:28 UTC；第四章正式评价 B+C 从 2024-08-19 00:28 开始，终止于 2025-01-01 前最后周期。旧图从 8 月 19 日起，是校准 A / 正式评价 B+C 的冻结分割起点；不是原始 Ping 数据起点。M1—M4 阈值使用 2024-07-29 至 2024-08-19 的校准 A；M5 用每周期严格先前的 14 日窗口，需 70% 历史覆盖，当前 RTT 至少 5 个有效样本。历史以周期之前数据构造，不读取未来。方法最早/最晚有效判定及未知原因见 `time_range_method_audit.csv`、`unknown_reason_counts.csv`。\n\n{md_table(time_audit(states)[['method','formal_eval_start_utc','formal_eval_end_utc','first_path_hour_decision_utc','last_path_hour_decision_utc','known_path_hour_n','unknown_path_hour_n']])}\n\n数据范围审计原记录：\n```json\n{scope_text}\n```\n\n## 方法规则与状态\n\n方法名及实现来源/正式阈值/历史与当前有效性条件见 `five_methods_threshold_and_source_audit.csv`。M1—M4 为项目中声明的研究适配方法，名称不声称复现原论文/系统；M5 为第四章 IPS+RTT 路径关联正式方法。绘图只映射已有正式输出：0=正常，1=方法已正式判异常，NA=方法依据不足；不改阈值、不插值、不平滑。已执行全量计划 Ping 扫描中零响应按正式 IPS 语义保留为未响应证据；RTT 缺失仍按 RTT 证据不足处理。逐小时结果和每个 NA 具体原因在结果 CSV。\n\n## 事件和图窗\n\n主图覆盖 2024-11-15 22:28 UTC 到 2024-11-18 21:28 UTC 的 72 个连续小时周期；外部事件只以 Kyiv 当地 2024-11-17 日期定位，转换后 UTC 日期区间为 [2024-11-16 22:00, 2024-11-17 22:00)。该日期级灰带不是事件持续时间或精确时刻。匹配对照为当地 2024-11-10，使用同一星期几、相同 72 小时布局，选择规则为邻近的此前星期日，不按异常表现挑选。长异常图由真实状态表中最长连续异常运行自动确定，按每段不超过 72 个连续小时分页；检测异常持续时间不等于外部事件持续时间。事件转换和夏令时核验见 `event_utc_dst_audit.csv`。\n\n## 同一共同可判定评价范围\n\n为控制五种方法有效覆盖差异，另以完全相同的路径—小时单元（五种方法均为已判定状态）计算正常/异常比例。该交集可能比任一方法单独覆盖范围更小，因此与全评价总体三态比例并列报告，不改变任何方法的未知状态或分母定义。\n\n{md_table(common_dist)}\n\n## 图像、复现和核验\n\nPDF 为每个时间窗的多页完整路径分页版；对应每页另有 SVG 与 400 dpi PNG。每页最多按 path_order 显示两条完整路径（末页可为一条），所有方法共享完全相同路径顺序与小时格；每格对应连续一小时周期，采样记录时间为每小时 :28 UTC。A4 横向页面分页展示完整 ASGeo 路径身份；72 小时图每 3 小时一个刻度，中间小时均保留。离散状态颜色固定为正常 `{COLORS[0]}`、异常 `{COLORS[1]}`、无法判定 `{COLORS[2]}`。\n\n路径完整身份和成员清单：`private/odesa_classI_path_crosswalk.csv`、`private/odesa_classI_full_member_crosswalk.csv`。区域筛选表、逐小时状态、全评价与共同可判定范围状态统计、事件/控制日统计、最长连续运行、时间范围和未知原因表均在 `results/oblast_classI_hourly_20261030/`。生成脚本：`code/rebuild_oblast_class1_hourly.py`。\n\n**限制：** 11 月 17 日外部材料为日期级线索；它不能证明州内每条路径异常由该事件导致。长期 11 月 28 日事件源没有确认敖德萨影响，因此图仅标记全国日期。不同方法判定覆盖率不同，异常比例应结合正常/异常/NA 三态和共同覆盖解释；不以未知当正常。原始零响应重建依赖“固定全量周期扫描”的既有项目假设，scope audit 记录已注明执行名册未独立恢复。\n'''
    REPORT.write_text(report,encoding='utf-8')
    with REPORT.open('a',encoding='utf-8') as f:
        f.write('\n\n## 历史不足与未知区间逐段审计\n\n')
        f.write('详细 NA 区间见 `results/oblast_classI_hourly_20261030/unknown_intervals.csv`；按明确含 history、historical、prior 或 baseline 原因码提取的区间见 `history_insufficiency_intervals.csv`。如果无该类原因，则表中列明每方法 0 小时；没有将缺测周期或当前证据不足误算为历史不足。\n')
        f.write('\n五种方法当前形式化历史不足区间：\n\n')
        f.write(md_table(history_intervals[['method','unknown_reason','start_utc','end_sample_utc','unknown_hours']]))
    print(json.dumps({'status':'done','selected_region':'敖德萨州','path_n':len(paths),'prefix24_n':int(paths.prefix24_n.sum()),'longest_run':manifest['longest_observed_run'],'figure_pages':len(pages),'figures':str(FIG),'results':str(OUT),'report':str(REPORT),'font':font},ensure_ascii=False,indent=2))

if __name__=='__main__': main()
