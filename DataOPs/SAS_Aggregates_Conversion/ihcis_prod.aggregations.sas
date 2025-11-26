options sasautos=(sasautos
                 '\\nasgw8315pn.ihcis.local\Prod\trunk\SASApps\Macros\Prod'
				 '\\NASGW8315PN.ihcis.local\Imp\Users\camendol\SAS_ETL_dev\support_lib'
				 '\\NASGW8315PN\Imp\Users\camendol\DataOPs\DCM\SAS_Aggregates_Conversion'
                 '!sasfolder\sasmacro'
                 );
/**
  * Macro to concatenate IHCIS WH 'prod' datasets
 */
%macro ihcis_prod_concat( prod_dir=
                         ,client=
						 ,pattern=
						 ,_out_ds=work._temp_);
						 
  libname &client._prod "&prod_dir.";						 
 
  %get_filenames( &prod_dir.
                 ,metadata=work.file_list
                 ,filter_regex=&pattern.);
				 
   proc sql noprint;
     select memname 
	          into :_ds_list 
			  separated by ' '
	   from work.file_list;
   quit;

  /* Appending all source file into one dataset */
  data &_out_ds./view=&_out_ds.;
    set %code_map(_ds_list,_ds_,%nrstr(&client._prod.%scan(&_ds_,1,.)));
  run; 

%mend ihcis_prod_concat; 

%macro freq_summ( table=
                 ,column_list=
				 ,by_var=
				 );
				 
    %local _enum;			
			
    %let sql_statement=;
	%let _enum=0;
	
	%macro sql_freq_st(column);
	
	    %if &_enum>0 %then %let sql_statement=&sql_statement.   UNION ALL ;
		%let _enum=%eval(&_enum+1);
		
		%let sql_statement=&sql_statement. SELECT &by_var,"&column." as variable,"Missing" as value,count(*) as freq FROM &table. WHERE &column. IS NULL GROUP BY &by_var.
                            UNION ALL
                            SELECT &by_var.,"&column." as variable,&column. as value,count(&column.) as freq FROM &table. WHERE &column. IS NOT NULL GROUP BY &column.,&by_var;
		
	%mend  sql_freq_st;

     	
	%mac_map(sql_freq_st,to_list=column_list)
 
    &sql_statement.

%mend freq_summ;
%macro drop_from( _list
                 ,items=);
  				   
  %let _raw_list=&&&_list.;

  %code_map( items
            ,x
            ,%nrstr(%let _raw_list=%sysfunc(prxchange(s/\b&x.\b//, -1, &_raw_list.));));				   
			
  %let &_list.=&_raw_list.;	
  
%mend drop_from;


%let client=ucd;
%let cycle=20200731;

%let src_dir=G:\data_warehouse\UC_Davis_OPA\PD20200731\prod;
%let summ_dir=\\apswp2286\W\camendol\SPC\data;
%let src_contract=plan_source;

/*PHM Dataset Aggregation*/
%ihcis_prod_concat( prod_dir=&src_dir.
                   ,client=ucd
	 			   ,pattern=phm20.{2}_ucd01.sas7bdat
				   ,_out_ds=work._temp_);
				   
/* Volumetrics summarization */
proc sql;
    create table prod_phm as
      select put(MONTH(dos),2.) as Mon
            ,put(YEAR(dos),4.) as Yr
		    ,count(*) as numrows_sum
		    ,count(distinct member) as members_sum
		    ,sum(amt_pay) as amt_pay_sum
		    ,sum(amt_req) as amt_req_sum
		    ,sum(amt_eqv) as amt_eqv_sum
		    ,sum(met_qty) as qty_sum
		    ,&src_contract. as contract  
	  from work._temp_ 
	  group by Yr
	       ,Mon
		   ,&src_contract.   
	   order by Yr
	       ,Mon
		   ,&src_contract.  ;			 
  quit;

  data prod_phm;
    set prod_phm;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;

%sas2sqlite( sastable = prod_phm
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);				   
				   			   
/* Parse dataset variables*/
%let _fields= dtsc_cd 
             dtsc_lv1 
             dtsc_lv2 
             dtsc_lv3 
             member 
             member_n_clm 
             covclass 
             sex 
             span 
prod_lv1 
prod_lv2 
prod_lv3 
keep_prd 
prod_org 
prod_lo 
prod_hi 
rti 
mem_type 
pcp 
pcp_grp_lo 
pcp_grp_mid 
pcp_grp_hi 
emp_lo 
emp_mid 
emp_hi 
ben_pkg 
pharmben 
span_typ 
phm_type_n 
prv 
pharmacy_name 
pres_prv 
prv_netwk 
pres_prv_netwk 
dea_num 
daw_flag 
ref_flag 
formlary 
ndc 
gbo_i 
gbo_n 
ms_ind 
tos_i 
tos_n 
plan_source 
denied_flg 
denied_ind 
pseudo_flg 
clm_source 
at_risk 
network_status_pres_prv 
network_status_prv 
pcp_affil 
plan_source_n  
network_paid_status 
provider_status 
contract_clm 
map_srce_e 
map_srce_p 
pcp_n 
pres_prv_n 
prv_n 
cap_ind 
spec_rx_n 
clm_srce 
clm_stat 
map_srce_n 
client_source 
mem_xwalk_match   
pres_prv_uid 
pres_prv_xwalk_match 
prv_uid 
prv_xwalk_match;

/* Field Level summarization */ 
proc sql;
   create table prod_phm_fields as
   %freq_summ( table=work._temp_ 
              ,column_list=&_fields.
		      ,by_var=&src_contract.);
quit;

%sas2sqlite( sastable = prod_phm_fields
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);	

/*Volumetrics: Prod Span*/
%ihcis_prod_concat( prod_dir=&src_dir
                   ,client=ucd
	 			   ,pattern=mspan_ucd01.sas7bdat
				   ,_out_ds=work._temp_);

data work.project_spans_months( keep=&src_contract.
                                     member 
                                     eff_dt 
									 end_dt
                                  drop=span_beg_dt 
                                      span_end_dt 
                                      effective_date 
                                      term_date);
    set work._temp_(rename=( eff_dt=effective_date 
	                               end_dt=term_date));

    format eff_dt 
           end_dt 
           span_beg_dt 
           span_end_dt yymmdd10.;

    rank_mem=_n_;

    span_beg_dt=intnx('month',effective_date,0,'s');
    span_end_dt=intnx('month',term_date,0,'s');
    /*Maybe limit span end date to current date?*/
    if span_end_dt >today() then span_end_dt=intnx('month',today(),0,'s');
  
    eff_dt=span_beg_dt;
    end_dt=intnx('month',eff_dt,0,'e');
    output;
  
    do while (eff_dt < span_end_dt);
   
      eff_dt=intnx('month',eff_dt,1,'s');
      end_dt=intnx('month',eff_dt,0,'e');
      output;
    
    end;
	
	proc sql;
	  create table prod_span as
	    select &src_contract. as contract 
              ,put(MONTH(eff_dt),2.) as Mon
              ,put(YEAR(eff_dt),4.) as Yr
        ,count(member) as mmos_sum
        ,count(distinct member) as members_sum
          from work.project_spans_months
          group by  Mon
                   ,Yr
				   ,&src_contract.
          order by  Yr
                   ,Mon
				   ,&src_contract.; 
	quit;
	
	data prod_span;
      set prod_span;
      array change _numeric_;
        do over change;
            if change=. then change=0;
        end;
    run;
	
    proc print data=prod_span(obs=25);
	run;
	
	%sas2sqlite( sastable = prod_span
              ,path = &summ_dir
			  ,database = &client._prod_&cycle._summ.sqlite); 			   

			
/*Volumetrics: Prod med */
%ihcis_prod_concat( prod_dir=&src_dir
                   ,client=ucd
	 			   ,pattern=med20.{2}_ucd01.sas7bdat
				   ,_out_ds=work._temp_);
				   				  				   
/* Volumetrics aggregation */
proc sql;
    create table prod_med as
      select put(MONTH(dos),2.) as Mon
            ,put(YEAR(dos),4.) as Yr
		    ,count(*) as numrows_sum
		    ,count(distinct member) as members_sum
		    ,sum(amt_pay) as amt_pay_sum
		    ,sum(amt_req) as amt_req_sum
		    ,sum(amt_eqv) as amt_eqv_sum
		    ,sum(qty) as qty_sum
		    ,&src_contract. as contract  
	  from work._temp_ 
	  group by Yr
	       ,Mon
		   ,&src_contract.   
	   order by Yr
	       ,Mon
		   ,&src_contract.  ;			 
  quit;

  data prod_med;
    set prod_med;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;
 
%sas2sqlite( sastable = prod_med
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);	

/* Parse dataset variables*/
%let _fields=dtsc_cd 
dtsc_lv1 
dtsc_lv2 
dtsc_lv3 
covclass 
span 
prod_lv1 
prod_lv2 
prod_lv3 
keep_prd 
prod_org 
prod_lo 
prod_hi 
rti 
mem_type 
pcp 
pcp_grp_lo 
pcp_grp_mid 
pcp_grp_hi 
emp_lo 
emp_mid 
emp_hi 
ben_pkg 
pharmben 
span_typ 
conf_flg 
bill_prv 
order_prv 
prv 
prv_sp_i 
prv_sp_n 
serv_prv_type 
prv_grp_lo 
prv_grp_mid 
prv_grp_hi 
prv_par 
prv_netwk 
cap_ind 
revenue 
revtype 
proccode 
proctype 
mod_n 
mod2_n 
mod3_n 
mod4_n 
icd_type 
iproc1 
iproc2 
iproc3 
iproc4 
iproc5 
iproc6 
iproc7 
iproc8 
iproc9 
iproc10 
admit_type_n 
bill_type_n 
dis_stat 
diag1 
diag2 
diag3 
diag4 
diag5 
diag6 
diag7 
diag8 
diag9 
diag10                 
drg_n 
drg_n_ver 
drg_n_level 
drg_n_outlier 
pos_i 
pos_n 
tos_i 
tos_n 
poa_n 
denied_flg 
denied_ind 
pseudo_flg  
admit_source
at_risk 
drg_n_payer
drg_version_payer 
drg_level_payer 
drg_outlier_payer 
drg_payer 
invasive 
network_status_prv 
pcp_affil 
plan_source_n 
refer_prv 
serv_affil 
serv_prv_spec1_n  
network_paid_status 
provider_status 
contract_clm 
map_srce_e 
map_srce_p 
taxonomy 
pcp_n 
prv_n 
bill_prv_n 
order_prv_n 
refer_prv_n 
spec_rx_n 
clm_srce 
clm_stat 
map_type 
map_srce_n 
Bill_prv_uid 
Bill_prv_xwalk_match 
client_source 
mem_xwalk_match   
order_prv_uid 
order_prv_xwalk_match 
pcp_uid 
pcp_xwalk_match 
prv_uid 
prv_xwalk_match 
refer_prv_uid 
refer_prv_xwalk_match;

/* Field Level summarization */ 
proc sql;
   create table prod_med_fields as
   %freq_summ( table=work._temp_ 
              ,column_list=&_fields.
		      ,by_var=&src_contract.);
quit;

%sas2sqlite( sastable = prod_med_fields
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);				

/*Volumetrics: Prod Inp */
%ihcis_prod_concat( prod_dir=&src_dir
                   ,client=ucd
	 			   ,pattern=inp20.{2}_ucd01.sas7bdat
				   ,_out_ds=work._temp_);
				   				  				   
/* Volumetrics aggregation */
proc sql;
    create table prod_inp as
      select put(MONTH(dos),2.) as Mon
            ,put(YEAR(dos),4.) as Yr
		    ,count(*) as numrows_sum
		    ,count(distinct member) as members_sum
		    ,sum(amt_pay) as amt_pay_sum
		    ,sum(amt_req) as amt_req_sum
		    ,sum(amt_eqv) as amt_eqv_sum
		    ,sum(qty) as qty_sum
		    ,&src_contract. as contract  
	  from work._temp_ 
	  group by Yr
	       ,Mon
		   ,&src_contract.   
	   order by Yr
	       ,Mon
		   ,&src_contract.  ;			 
  quit;

  data prod_inp;
    set prod_inp;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;
 
%sas2sqlite( sastable = prod_inp
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);	

/* Parse dataset variables*/
%let _fields=dtsc_cd 
dtsc_lv1 
dtsc_lv2 
dtsc_lv3 
covclass 
span 
prod_lv1 
prod_lv2 
prod_lv3 
keep_prd 
prod_org 
prod_lo 
prod_hi 
rti 
mem_type 
pcp 
pcp_grp_lo 
pcp_grp_mid 
pcp_grp_hi 
emp_lo 
emp_mid 
emp_hi 
ben_pkg 
pharmben 
span_typ 
conf_flg 
bill_prv 
order_prv 
prv 
prv_sp_i 
prv_sp_n 
serv_prv_type 
prv_grp_lo 
prv_grp_mid 
prv_grp_hi 
prv_par 
prv_netwk 
cap_ind 
revenue 
revtype 
proccode 
proctype 
mod_n 
mod2_n 
mod3_n 
mod4_n 
icd_type 
iproc1 
iproc2 
iproc3 
iproc4 
iproc5 
iproc6 
iproc7 
iproc8 
iproc9 
iproc10 
admit_type_n 
bill_type_n 
dis_stat 
diag1 
diag2 
diag3 
diag4 
diag5 
diag6 
diag7 
diag8 
diag9 
diag10                 
drg_n 
drg_n_ver 
drg_n_level 
drg_n_outlier 
pos_i 
pos_n 
tos_i 
tos_n 
poa_n 
denied_flg 
denied_ind 
pseudo_flg  
admit_source
at_risk 
drg_n_payer
drg_version_payer 
drg_level_payer 
drg_outlier_payer 
drg_payer 
invasive 
network_status_prv 
pcp_affil 
plan_source_n 
refer_prv 
serv_affil 
serv_prv_spec1_n  
network_paid_status 
provider_status 
contract_clm 
map_srce_e 
map_srce_p 
taxonomy 
pcp_n 
prv_n 
bill_prv_n 
order_prv_n 
refer_prv_n 
spec_rx_n 
clm_srce 
clm_stat 
map_type 
map_srce_n 
Bill_prv_uid 
Bill_prv_xwalk_match 
client_source 
mem_xwalk_match   
order_prv_uid 
order_prv_xwalk_match 
pcp_uid 
pcp_xwalk_match 
prv_uid 
prv_xwalk_match 
refer_prv_uid 
refer_prv_xwalk_match;

/* Field Level summarization */ 
proc sql;
   create table prod_inp_fields as
   %freq_summ( table=work._temp_ 
              ,column_list=&_fields.
		      ,by_var=&src_contract.);
quit;

%sas2sqlite( sastable = prod_inp_fields
            ,path = &summ_dir
		    ,database = &client._prod_&cycle._summ.sqlite);				