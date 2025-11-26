library(data.table)
set.seed(10)

qlab1<-'Which of the following statements closest represents your opinion about telecommuting in general?'
  
qlab2<-'How long have you been telecommuting in your current role?'
  
qlab3<-'How much time have you spent working in fully in-office situations(Career Total)?'
  
qlab4<-'What is your general job function/role(2-3 words)?'
  
qa1<-c( "Telecommuting is a nice concept but doesn't work in the real world"
       ,"Telecommuting is fantastic! We can do everything from home - see our kids and benefit society in many ways"
       ,"Telecommuting is a disaster, I am zoomed-out, and lack of contact with others is making me feel like I am in solitary confinement"
       ,"Telecommuting is an appropriate method for many kinds of work, with proper implementation and support")

a1_p<-c(.15,.30,.20,.35)


qa2<-c( "Less Than a Year"
       ,"1-2 Years"
       ,"2-3 Years"
       ,"More Than 3 Years")
a2_p<-c(.25,.25,.25,.25)

qa3<-c( "Never"
       ,"1-2 Years"
       ,"3-4 Years"
       ,"5-6 Years"
       ,"More Than 6 Years")
a3_p<-c(.10,.25,.25,.25,.15)

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
a4_p<-c(.20,.05,.05,.05,.15,.05,.1,.05,.1,.05,.10,.05)

rows_ct<-250

q1_r<-sample( x=qa1 
             ,prob=a1_p
             ,size=rows_ct
             ,replace=TRUE)

q2_r<-sample( x=qa2 
              ,prob=a2_p
              ,size=rows_ct
              ,replace=TRUE)

q3_r<-sample( x=qa3 
              ,prob=a3_p
              ,size=rows_ct
              ,replace=TRUE)

q4_r<-sample( x=qa4 
              ,prob=a4_p
              ,size=rows_ct
              ,replace=TRUE)

final_data<-data.table( q1=q1_r
                       ,q2=q2_r
                       ,q3=q3_r
                       ,q4=q4_r)

final_data$q1_short<-ifelse( final_data$q1=="Telecommuting is a nice concept but doesn't work in the real world"
                             ,'Nice Concept'
                             ,ifelse(final_data$q1=="Telecommuting is fantastic! We can do everything from home - see our kids and benefit society in many ways"
                                     ,'Fantastic!'
                                     ,ifelse( final_data$q1=="Telecommuting is a disaster, I am zoomed-out, and lack of contact with others is making me feel like I am in solitary confinement"
                                              ,'Disaster'
                                              ,ifelse( final_data$q1=="Telecommuting is an appropriate method for many kinds of work, with proper implementation and support"
                                                       ,'Appropriate'
                                                       ,final_data$q1))))

fwrite(final_data,'C:/Users/chris/Documents/Optum/Telecommuter/show_and_tell')