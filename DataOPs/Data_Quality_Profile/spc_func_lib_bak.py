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
    
    inspect=combined.loc[  ( (combined['phi_x']>=effect_size_threshold)\
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
    print('Number of cell selected for inspection:',len(inspect))
    print('Significance Level: ',significance_level)
    print('Effect Size Threshold: ',effect_size_threshold)
    
    return inspect 

def volume_test_pc( current_df\
                  ,prior_df\
                  ,vkey\
                  ,vcols\
                  ,sig=.01\
                  ,err_level=.04\
                  ,cmp_beg_dt='2017-01-01'
                  ,pri_lab='pri'
                  ,cur_lab='cur'):
    
    import pandas as pd
    import numpy as np
    import scipy.stats as stats
    
    p_test_value=1-sig
    
    #CAA: Must date filter before merge or we get weird cartesian effects for unmatched yr-mons
    
    final={}
    
    #Set-up difference dataframe
    diffs=prior_df.merge( current_df
                         ,how='outer'\
                         ,left_on=vkey\
                         ,right_on=vkey\
                         ,suffixes=('_'+pri_lab, '_'+cur_lab))\
                  .fillna(0)\
                  .reset_index(drop=True)
    
    diffs['filter_date']=pd.to_datetime( diffs['Yr'].astype(str) + diffs['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                        ,format='%Y%m%d'
                                        ,errors='ignore')									
    
    #Get aggregates for diffs leaving out last 3 months, aka run-out
    diffs_filtered=diffs.groupby([vkey[0]])\
                        .apply(lambda x: x.iloc[:-3])\
                        .reset_index(drop=True)
    
    #Trim off early dates prior to comparison range
    diffs_filtered=diffs_filtered[diffs_filtered['filter_date'] >= pd.to_datetime(cmp_beg_dt)]
    
    diff_list=[]
    for measure in vcols:
        diffs_filtered[measure+'_dif']=diffs_filtered[measure+'_sum_cur']-diffs_filtered[measure+'_sum_pri']
        diffs_filtered[measure+'_dev']=abs(diffs_filtered[measure+'_sum_cur']-diffs_filtered[measure+'_sum_pri'])
        diffs_filtered[measure+'_dev_pop']=err_level*diffs_filtered[measure+'_sum_pri']
       
        diff_list.append((measure+'_sum_pri'))
        diff_list.append((measure+'_dev'))
        diff_list.append((measure+'_dev_pop'))
        diff_list.append((measure+'_dif'))
                
    filter_cols=vkey+diff_list
    
    #Create lookup for Simple Tukey Outlier upper limit
    out_data=[]
    print(vkey[0])
    for contract in diffs_filtered[vkey[0]].unique():
        print('---',contract)
        devs=diffs_filtered\
               .loc[diffs_filtered[vkey[0]]==contract]\
               .filter(regex='_dev$',axis=1)
        
        row=[]
        row.append(contract)
        
        for measure in vcols:
            
            out_ul=(( devs[measure+'_dev'].quantile(q=0.75, interpolation='midpoint')\
                     -devs[measure+'_dev'].quantile(q=0.25, interpolation='midpoint'))\
                    *3.0)\
                    +devs[measure+'_dev'].quantile(q=0.75, interpolation='midpoint')
            row.append(out_ul)
      
        out_data.append(row)
        
    temp_header=[] 
    temp_header.append(vkey[0])
    
    for measure in vcols:
        temp_header.append(measure+'_out_ul')
            
    outlier_uls=pd.DataFrame( out_data\
                             ,columns = temp_header)
    
    out_data=[]
    temp_header=[]
    
    #Merge Outlier limits onto the to be screened data
    out_raw=diffs_filtered.set_index(vkey[0])\
                          .join(outlier_uls.set_index(vkey[0]))
    
    #Compare Raw values to outlier upperlimit - flag is numeric to be able to sum occurances
    outliers_cols=[]
    for measure in vcols:
        out_raw[measure+'_outlier']=np.where( out_raw[measure+'_dev']>out_raw[measure+'_out_ul']\
                                             ,1\
                                             ,0)
        out_raw[measure+'_label']=np.where( out_raw[measure+'_dev']>out_raw[measure+'_out_ul']\
                                             ,'**'\
                                             ,' ')
        
        outliers_cols.append((measure+'_outlier'))
        diff_list.append((measure+'_outlier'))
    
    final['_raw_']=out_raw.reset_index()
    
    out_raw.reset_index(inplace=True)
    
    #Aggregates of 'stable period'
    diffs_agg=out_raw\
              .groupby(vkey[0])\
              .agg(['mean','count','std','sum','sem'])\
              .loc[:,diff_list]
    
    for measure in vcols:
        #Significance testing of mean deviation from previous greater than error range
        diffs_agg[(measure+'_dev'),('ucl')]=diffs_agg[(measure+'_dev'),('mean')]\
                                            +(diffs_agg[(measure+'_dev'),('sem')]*1.96)
        
        diffs_agg[(measure+'_dev_pop'),('ucl')]=diffs_agg[(measure+'_dev_pop'),('mean')]\
                                            +(diffs_agg[(measure+'_dev_pop'),('sem')]*1.96)
        
        diffs_agg[(measure+'_dev'),('sig')]=np.where( diffs_agg[(measure+'_dev'),('ucl')]\
                                                     >diffs_agg[(measure+'_dev_pop'),('ucl')]\
                                                             ,'SIG'
                                                             ,'NS')
        
        #Test for greater variance in observed deviances a compared to allowed variance for 'error'
        diffs_agg[(measure+'_dev'),('var')]=diffs_agg[(measure+'_dev'),('sem')]**2
        diffs_agg[(measure+'_dev_pop'),('var')]=diffs_agg[(measure+'_dev_pop'),('std')]**2
        
        diffs_agg[(measure+'_dev'),('var_chi')]=(   (diffs_agg[(measure+'_dev'),('count')])\
                                                   *(diffs_agg[(measure+'_dev'),('var')]) )\
                                                 /(diffs_agg[(measure+'_dev_pop'),('var')])
        
        diffs_agg[(measure+'_dev'),('var_crit')]=stats.chi2.ppf( .99,diffs_agg[(measure+'_dev'),('count')])
        diffs_agg[(measure+'_dev'),('var_sig')]=np.where( diffs_agg[(measure+'_dev'),('var_chi')]\
                                                           >diffs_agg[(measure+'_dev'),('var_crit')]\
                                                           ,'SIG'\
                                                           ,'NS')
        
        diffs_agg[(measure+'_dev'),('outliers')]=np.where( diffs_agg[(measure+'_outlier'),('sum')]>0\
                                                         ,'YES'\
                                                         ,'NO')
        
    final['_agg_']=diffs_agg
    
    stack=diffs_agg.stack(0)
    
    final['_screened_']=stack.loc[(stack['sig']=='SIG') | (stack['var_sig']=='SIG')]
    #| (stack['outliers']=='YES')]
    
    return final

def vol_test_plot( screen_results
                  ,list_only=False
                  ,vkey0='contract'):
    
    import matplotlib.pyplot as plt
    from pandas.plotting import table
    
    for plot_by,extra in screen_results['_screened_'][['var_chi','var_crit','sig','outliers']]\
                                        .groupby(level=[0,1]):
        
        source=plot_by[0]
        
        var=plot_by[1].replace('_dev','')
        
        lookup=screen_results['_agg_']
        var_chi=lookup.loc[lookup.index==source,(plot_by[1],('var_chi'))].values
        var_crit=lookup.loc[lookup.index==source,(plot_by[1],('var_crit'))].values
        sig=lookup.loc[lookup.index==source,(plot_by[1],('sig'))].values
        outliers=lookup.loc[lookup.index==source,(plot_by[1],('outliers'))].values
    
        title=' '+str(source)+' '+var\
             +'    CHI: '+str(var_chi)\
             +' CRIT: '+str(var_crit)\
             +'  MEAN: '+str(sig[0])
        
        if list_only:
            title=title+' OUTLIERS: '+str(outliers)
            print(title)
        else: 
            temp_df=screen_results['_raw_']
    
            temp_df['Yr_Mon']=temp_df['Mon'].map(str)+'-'+temp_df['Yr'].map(str)
    
            fig, ax = plt.subplots(1, 1)
    
            contract_data=temp_df.loc[ temp_df[vkey0]==plot_by[0]\
                                 ,( 'Yr_Mon'\
                                   ,var+'_sum_pri'\
                                   ,var+'_sum_cur'\
                                   ,var+'_label')]
    
            contract_data.plot( ax=ax\
                           ,kind='bar'\
                           ,x='Yr_Mon'\
                           ,title=title)
    
            plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    
            xc=(len(ax.patches)/2)-1
    
            for idx,patch in enumerate(ax.patches):
                if idx <= xc:
                    bl = patch.get_xy()
                    x = 1.4 * patch.get_width() + bl[0]
                    y = 1.01 * patch.get_height() + bl[1] 
                    ax.text( x\
                            ,y\
                            ,contract_data[var+'_label'].values[idx]\
                            ,ha='center'\
                            ,rotation='horizontal'\
                            ,weight = 'normal')
                           
def vol_diff_plot( screen_results
                  ,list_only=False
                  ,vkey0='contract'):
    
    import matplotlib.pyplot as plt
    from pandas.plotting import table
    
    for plot_by,extra in screen_results['_screened_'][['var_chi','var_crit','sig','outliers']]\
                                        .groupby(level=[0,1]):
        
        source=plot_by[0]
        
        var=plot_by[1].replace('_dev','')
        
        lookup=screen_results['_agg_']
        var_chi=lookup.loc[lookup.index==source,(plot_by[1],('var_chi'))].values
        var_crit=lookup.loc[lookup.index==source,(plot_by[1],('var_crit'))].values
        sig=lookup.loc[lookup.index==source,(plot_by[1],('sig'))].values
        outliers=lookup.loc[lookup.index==source,(plot_by[1],('outliers'))].values
    
        title=' '+str(source)+' '+var\
             +'    CHI: '+str(var_chi)\
             +' CRIT: '+str(var_crit)\
             +'  MEAN: '+str(sig[0])
        
        if list_only:
            title=title+' OUTLIERS: '+str(outliers)
            print(title)
        else: 
            temp_df=screen_results['_raw_']
    
            temp_df['Yr_Mon']=temp_df['Mon'].map(str)+'-'+temp_df['Yr'].map(str)
    
            fig, ax = plt.subplots(1, 1)
    
            contract_data=temp_df.loc[ temp_df[vkey0]==plot_by[0]\
                                 ,( 'Yr_Mon'\
                                   ,var+'_dif'
                                   ,var+'_label')]
    
            contract_data.plot( ax=ax\
                           ,kind='bar'\
                           ,x='Yr_Mon'\
                           ,title=title)
    
            plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    
            xc=(len(ax.patches)/2)-1
    
            for idx,patch in enumerate(ax.patches):
                if idx <= xc:
                    bl = patch.get_xy()
                    x = 1.4 * patch.get_width() + bl[0]
                    y = 1.01 * patch.get_height() + bl[1] 
                    ax.text( x\
                            ,y\
                            ,contract_data[var+'_label'].values[idx]\
                            ,ha='center'\
                            ,rotation='horizontal'\
                            ,weight = 'normal')                           