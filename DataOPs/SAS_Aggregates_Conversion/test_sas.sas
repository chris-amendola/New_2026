%let src_contract=PLAN_SOURCE;

libname _src_ '\\apswp2287\J\Data_Warehouse\Reliant\PD20210228\base'  ;
libname temp 'W:\camendol\SPC\sas_test_data';

data temp.project_spans_months( keep=&src_contract.
                                     member 
                                     eff_dt 
									 end_dt
                                  drop=span_beg_dt 
                                      span_end_dt 
                                      effective_date 
                                      term_date);
    set _src_.rmg_cur_elgcms_span(rename=( eff_dt=effective_date 
	                                       end_dt=term_date));

    format eff_dt 
           end_dt 
           span_beg_dt 
           span_end_dt yymmdd10.;

    rank_mem=_n_;

    span_beg_dt=intnx('month',effective_date,0,'s');
    span_end_dt=intnx('month',term_date,0,'e');
	
    /*Maybe limit span end date to current date?*/
    if span_end_dt >today() then span_end_dt=intnx('month',today(),0,'e');
    
    eff_dt=span_beg_dt;
    end_dt=intnx('month',eff_dt,0,'e');
    output;
  
    do while (END_DT < span_end_dt);
           
           EFF_DT=intnx('month',EFF_DT,1,'s');
           END_DT=intnx('month',END_DT,0,'e');
           output;
    
         end;
run;	

proc sql;
create table test as
select PLAN_SOURCE as contract 
              ,put(MONTH(eff_dt),2.) as Mon
              ,put(YEAR(eff_dt),4.) as Yr
        ,count(Member) as dmos_sum
        ,count(distinct Member) as distinct_sum
          from work.project_spans_months
          group by  Mon
                   ,Yr
				   ,PLAN_SOURCE
          order by  Yr
                   ,Mon
				   ,PLAN_SOURCE; 
quit;     