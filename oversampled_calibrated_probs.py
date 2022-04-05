## This function calibrates probability values for oversampled forecasts. based on true Pos/Neg rate.
## data: dataframe
## R1: true target rate    (Pos / (pos + neg))
## R2: sample target rate  

def oversampled_prob_calib(data, R1, R2):

    import random
    
    data[["calibrated_probs"]] =  1/ ( 1 + ( ( (1/R1) -1  ) / ( (1/R2) -1  ) * ( ( 1 /data[["predictions"]]) -1 )  )  )

    return(calibrated_probabilities)

