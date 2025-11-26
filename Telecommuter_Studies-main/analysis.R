#TODO: Cross-Tabs of q1 by others 

setwd('C:/Users/chris/Documents/Optum/Telecommuter')

qlab1<-'Which of the following statements closest represents your opinion about telecommuting in general?'

qlab2<-'How long have you been telecommuting in your current role?'

qlab3<-'How much time have you spent working in fully in-office situations(Career Total)?'

qlab4<-'What is your general job function/role(2-3 words)?'

qa1<-c( "Telecommuting is a nice concept but doesn't work in the real world"
        ,"Telecommuting is fantastic! We can do everything from home - see our kids and benefit society in many ways"
        ,"Telecommuting is a disaster, I am zoomed-out, and lack of contact with others is making me feel like I am in solitary confinement"
        ,"Telecommuting is an appropriate method for many kinds of work, with proper implementation and support")

qa2<-c( "Less Than a Year"
        ,"1-2 Years"
        ,"2-3 Years"
        ,"More Than 3 Years")

qa3<-c( "Never"
        ,"1-2 Years"
        ,"3-4 Years"
        ,"5-6 Years"
        ,"More Than 6 Years")

qa4<-c( "project manager"
        ,"senior manager"
        ,"software developer"
        ,"software engineer"
        ,"client services"
        ,"clinical services"
        ,"data analyst"
        ,"data scientist"
        ,"product manager"
        ,"sales manager"
        ,'Account coordinator'
        ,'security analyst'
)

library(ggplot2)
library(data.table)

final_data<-fread('C:/Users/chris/Documents/Optum/Telecommuter/show_and_tell')

final_data$q1_short<-factor( final_data$q1_short
                             ,levels=c( 'Nice Concept'
                                        ,'Fantastic!'
                                        ,'Disaster'
                                        ,'Appropriate')
                             ,ordered=TRUE)

final_data$q2<-ordered( final_data$q2
                        ,levels=c( "Less Than a Year"
                                   ,"1-2 Years"
                                   ,"2-3 Years"
                                   ,"More Than 3 Years"))

final_data$q3<-ordered( final_data$q3
                        ,levels=qa3)


q1_sum<-final_data[,.(q1_fq=.N)
                   ,by=c('q1_short')]

q2_sum<-final_data[,.(q2_fq=.N)
                   ,by=c('q2')]

q3_sum<-final_data[,.(q3_fq=.N)
                   ,by=c('q3')]

q4_sum<-final_data[,.(q4_fq=.N)
                   ,by=c('q4')]

ggplot( q1_sum
       ,aes( x=q1_short
            ,y=q1_fq),group=q1_sum)+
        geom_bar(stat="identity")+
        ggtitle(qlab1)+ 
        xlab("Response")+
        ylab("Count")#+
        #theme_minimal()

ggplot( q2_sum
        ,aes( x=q2
              ,y=q2_fq))+
         geom_bar(stat="identity")+
         ggtitle(qlab2)+ 
         xlab("Response")+
         ylab("Count")+
         theme_minimal()

ggplot( q3_sum
        ,aes( x=q3
              ,y=q3_fq))+
  geom_bar(stat="identity")+
  ggtitle(qlab3)+ 
  xlab("Response")+
  ylab("Count")+
  theme_minimal()

ggplot( q4_sum
        ,aes( x=q4
              ,y=q4_fq))+
  geom_bar(stat="identity")+
  ggtitle(qlab4)+ 
  xlab("Response")+
  ylab("Count")+
  theme_minimal()+ coord_flip()