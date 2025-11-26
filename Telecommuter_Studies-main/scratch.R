xtab_q2<-final_data[,.(fq=.N)
                   ,by=c('q1_short','q2')]

xtab_q3<-final_data[,.(fq=.N)
                     ,by=c('q1_short','q3')]

ggplot( xtab_q2
        ,aes( x=q1_short
             ,y=fq
             ,fill=q2))+
  geom_bar( position="dodge"
           ,stat="identity")+
  labs(fill="Length in Role")+
  xlab("Opinion Telecommuting")+
  ylab("Count")+
  theme_bw()

ggplot( xtab_q3
        ,aes( x=q1_short
             ,y=fq
             ,fill=q3))+
  geom_bar( position="dodge"
           ,stat="identity")+
  labs(fill="Full Office Experience")+
  xlab("Opinion Telecommuting")+
  ylab("Count")+
  theme_bw()
