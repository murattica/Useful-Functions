## When target is a factor, it is accepted as a classification problem. 
cart_transform <- function(data, targ_column, columns,control = rpart.control(minsplit = 20, minbucket = 6, xval=10, maxdepth = 3) ) {
  
  library(dplyr)
  library(readr)
  library(rpart)
  library(rattle)
  library(rpart.plot)
  library(RColorBrewer)
  
  colnames(data)[colnames(data) == targ_column] <- "target"
  
  fit <- list()
  
  
  for (i in 1:length(columns)) {
    print(i)
    if (columns[i]!= "target") {
      df <- data[c(columns[i],"target") ]
      fit[i] <- list(rpart(target ~ ., data = df,parms = list(split = "gini"), control = control))     
    }
  }
  
  return(fit)
  
}

