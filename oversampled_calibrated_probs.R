## This function calibrates probability values for oversampled forecasts. based on true Pos/Neg rate.
## data: dataframe
## R1: true target rate    (Pos / (pos + neg))
## R2: sample target rate  



oversampled_prob_calib <- function(data, R1, R2) {
    
  calibrated_probabilities =  as.data.frame( 1/ ( 1 + ( ( (1/R1) -1  ) / ( (1/R2) -1  ) * ( ( 1 /data$predictions) -1 )  )  ))
  colnames(calibrated_probabilities) = 'calibrated_probs'
  
  return(calibrated_probabilities)
}
