def field_fit_pc( cur_frqs\
                 ,pri_frqs\
                 ,by_var\
                 ,effect_size_threshold=.1\
                 ,significance_level=0.01\
                ):
    
    import numpy as np
    from scipy.stats import chi2
    
    chi_crit=chi2.ppf( (1-significance_level),1)
    
    cur_summary=cur_frqs[['freq',by_var,'variable']]\
        .groupby([by_var,'variable'])\
        .sum()\
        .reset_index()\
        .rename(columns={'freq':'total',})
    
    pri_summary=pri_frqs[['freq',by_var,'variable']]\
        .groupby([by_var,'variable'])\
        .sum()\
        .reset_index()\
        .rename(columns={'freq':'total',})
    
    #Merge Total Counts to Freqs
    #Calculate Proportions/Percents
    prior_prop=pri_frqs.merge( pri_summary\
                           ,left_on=[by_var,'variable']\
                           ,right_on=[by_var,'variable']\
                           ,suffixes=('_ct', '_sum'))
   
    cur_prop=cur_frqs.merge( cur_summary\
                           ,left_on=[by_var,'variable']\
                           ,right_on=[by_var,'variable']\
                           ,suffixes=('_ct', '_sum'))
    
    combined=prior_prop.merge( cur_prop\
                          ,left_on=[by_var,'variable','value']\
                          ,right_on=[by_var,'variable','value']\
                          ,suffixes=('_pri','_cur'))\
                   .fillna('0')
    
    #Basic goodness of fit
    # Selected Pearsons equation for simplicity - at these count sizes it is fine, runs slightly faster,
    # np.log is a touch slow, and the effect size literature seems more extensive for this formulations
    
    combined['pri_prop']=(combined['freq_pri']/combined['total_pri']*1.0000)  
    combined['cur_prop']=(combined['freq_cur']/combined['total_cur']*1.0000)

    combined['exp_count_val']=(combined['pri_prop'])*combined['total_cur']
    combined['exp_count_oth']=(1-combined['pri_prop'])*combined['total_cur']
    
    combined['obs_count_val']=combined['freq_cur']
    combined['obs_count_oth']=combined['total_cur']-combined['freq_cur']
  
    combined['prop_diff']=combined['cur_prop']-combined['pri_prop']
    freqs={}
    #Create simple missing values summary
    freqs['_missing_']=combined.loc[combined['value']=='Missing']  
      
    #if exp=0 and obs=0 then x=0 - not expected, not found
    #if exp=0 and obs>0 then x={abitrarily large number} - not expected and found-
    # when a new category shows up this is automatically a cause for review    
    combined['x_val']=np.where((combined['exp_count_val']==0)\
                               ,np.where( (combined['obs_count_val']==0)\
                                          ,0\
                                          ,1000000\
                                        )\
                               ,((combined['obs_count_val']-combined['exp_count_val'])**2)\
                                 /combined['exp_count_val']\
                               )
                                       
    combined['x_oth']=np.where((combined['exp_count_oth']==0)\
                               ,np.where( (combined['obs_count_oth']==0)\
                                          ,0\
                                          ,1000000\
                                        )\
                               ,((combined['obs_count_oth']-combined['exp_count_oth'])**2)\
                                 /combined['exp_count_oth']\
                               )        
  
    combined['x_final']=combined['x_val']+combined['x_oth']
    
    combined['phi_x']=(combined['x_final']/combined['total_cur'])**(1/2)
    
    combined['chi_crit']=chi_crit
    
    freqs['_inspect_']=combined.loc[  ( (combined['phi_x']>=effect_size_threshold)\
                                      & (combined['variable']!='MEMBER')\
                                      )\
                                    ,( by_var\
                                    ,'variable'\
                                    ,'value'\
                                    ,'freq_pri'\
                                    ,'freq_cur'\
                                    ,'pri_prop'\
                                    ,'cur_prop'\
                                    ,'exp_count_val'\
                                    ,'obs_count_val'\
                                    ,'exp_count_oth'\
                                    ,'obs_count_oth'\
                                    ,'x_final'\
                                    ,'phi_x'\
                                    ,'x_val'\
                                    ,'chi_crit'\
                                    ,'x_oth'\
                           )]\
                    .sort_values( by=['phi_x']\
                                 ,ascending=False)
    
    print('Total number of cells: ',len(combined))
    print('Number of cell selected for inspection:',len(freqs['_inspect_']))
    print('Significance Level: ',significance_level)
    print('Effect Size Threshold: ',effect_size_threshold)
    
    return freqs 
    
def volume_test_pc( cur\
                   ,pri\
                   ,vkey\
                   ,vcols\
                   ,sig=.01\
                   ,err_level=.04\
                   ,cmp_beg_dt='2017-01-01'
                   ,pri_lab='prix'
                   ,cur_lab='curx'):
    
    import pandas as pd
    import scipy.stats as stats
    
    return_obj={}
    
    # Create a narrow dataset for pivots
    
    
    diffs_prep=pri.merge( cur
                     ,how='outer'\
                     ,left_on=vkey\
                     ,right_on=vkey\
                     ,suffixes=('_'+pri_lab, '_'+cur_lab))\
                               .fillna(0)\
                               .reset_index(drop=True)
                  
    diffs_prep['filter_date']=pd.to_datetime( diffs_prep['Yr'].astype(str) + diffs_prep['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                         ,format='%Y%m%d'
                                         ,errors='ignore')	
    
    #Get aggregates for diffs leaving out last 3 months, aka run-out
    diffs_filtered=diffs_prep.groupby([vkey[0]])\
                             .apply(lambda x: x.iloc[:-3])\
                             .reset_index(drop=True)
    
    #Trim off early dates prior to comparison range
    diffs_filtered=diffs_filtered[diffs_filtered['filter_date'] >= pd.to_datetime(cmp_beg_dt)]

    #Compute deviations - direction-less differeces
    dev_list=[]
    for measure in vcols:
        # Deviation=absolute value of difference between prior and current cycle values
        # Prior is 'expected' and current is obsereved
        diffs_filtered[measure+'_dev']= abs(diffs_filtered[measure+'_sum_'+cur_lab]\
                                       -diffs_filtered[measure+'_sum_'+pri_lab])
    
        # Create theoretical observed values based on a simple error value 
        # 0<Error<1   
        diffs_filtered[measure+'_err']=diffs_filtered[measure+'_sum_'+pri_lab]*err_level\
                                       +diffs_filtered[measure+'_sum_'+pri_lab]
                                   
        # Deviation of expected values from theoretical observered.                            
        diffs_filtered[measure+'_ul']=abs( diffs_filtered[measure+'_err']\
                                          -diffs_filtered[measure+'_sum_'+pri_lab])  

        # X^2=SUM((O-E)^2)/E)
        # Chi for the actual observed values
        diffs_filtered[measure+'_chi_act']=(diffs_filtered[measure+'_dev']**2)/diffs_filtered[measure+'_sum_'+pri_lab]   
        # Chi for theortical 'error' values
        diffs_filtered[measure+'_chi_err']=(diffs_filtered[measure+'_ul']**2)/diffs_filtered[measure+'_sum_'+pri_lab]                       
                                   
    
        dev_list.append((measure+'_dev'))
        dev_list.append((measure+'_ul'))
        dev_list.append((measure+'_chi_act'))   
        dev_list.append((measure+'_chi_err')) 

        #Diffs for ad-hoc analysis
        diffs_filtered[measure+'_dif']= diffs_filtered[measure+'_sum_'+cur_lab]\
                                       -diffs_filtered[measure+'_sum_'+pri_lab]                       

    dev_list.append(vkey[0]) 
    #Aggregates of 'stable period' variables - by Year-Mon
    diffs_agg=diffs_filtered[dev_list]\
               .groupby(vkey[0])\
               .agg(['mean','count','std','sum'])
          
    analysis_df = pd.DataFrame(columns=[ vkey[0]
                                        ,'measure'
                                        ,'dev_mean'
                                        ,'ul_mean'
                                        ,'p_0'
                                        ,'p_e'
                                        ,'actual_variance'
                                        ,'error_variance'
                                        ,'var_ratio'
                                        ,'critical_val']
                               )
    #Deal with by var?
    for group in diffs_filtered[vkey[0]].unique():
        
        group_agg=diffs_agg.loc[diffs_agg.index==group]
        group_dat=diffs_filtered[diffs_filtered[vkey[0]]==group]
       
        # then for each measure
        for measure in vcols:
           
            p_0=stats.ttest_1samp( group_dat[measure+'_dev']
                                  ,0
                                  ,alternative='greater')
            
            p_e=stats.ttest_1samp( group_dat[measure+'_dev']
                                  ,group_agg[(measure+'_ul'),('mean')]
                                  ,alternative='greater')
            
            # Compare ratio of chi^2 actual the theoretical error - follows an F-dsitirbution
            if group_agg[measure+'_chi_act']['sum'][0] > 0:
            
                act_var=group_agg[measure+'_chi_act']['sum'][0]/group_agg[measure+'_chi_act']['count'][0]
                err_var=group_agg[measure+'_chi_err']['sum'][0]/group_agg[measure+'_chi_err']['count'][0]
        
                var_ratio=(diffs_agg[measure+'_chi_act']['sum'][0]/diffs_agg[measure+'_chi_act']['count'][0]-1)\
                        / (diffs_agg[measure+'_chi_err']['sum'][0]/diffs_agg[measure+'_chi_err']['count'][0]-1)
            
                crit_val=stats.f.ppf( 1-sig
                                     ,diffs_agg[measure+'_chi_act']['count'][0]-1
                                     ,diffs_agg[measure+'_chi_err']['count'][0]-1 )
            
            row= [ group
                  ,measure
                  ,group_agg[measure+'_dev']['mean'][0]
                  ,group_agg[measure+'_ul']['mean'][0]
                  ,p_0[1]
                  ,p_e[1]
                  ,act_var
                  ,err_var
                  ,var_ratio
                  ,crit_val]
        
            analysis_df.loc[len(analysis_df)] = row
        
    return_obj['_raw_']=diffs_filtered
    return_obj['_stats_']=diffs_agg
    return_obj['_results_']=analysis_df
    return return_obj