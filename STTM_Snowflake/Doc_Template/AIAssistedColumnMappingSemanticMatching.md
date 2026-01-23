Use schema level information, ie table and column names as input to a large language model, to encode ‘meaning’ aka ‘semantic meaning’ for of schema information.

Source column meanings will be compared to target meanings (cross join), via some vector math, which tells us how similar in meaning source table-columns are to the target table columns, in the form of a confidence score that ranges theoretically for -1 to 1. 

So comparing ‘patient’ to ‘patient’ would get you a 1.0 confidence. Comparing ‘patient’ to ‘person’ might get you a 0.6. ‘Doctor’ to ‘Physician’ maybe 0.85.

That confidence score can thresh-hold-ed and rank-ordered to provide a top-n recommendations list for each target column.



LLM models are trained to understand how words go together, not just in pairs but in 10K at once.

Trained on insane amounts of raw data, via insanely complex neural networks. 

Trained models are really more or less ‘plug and chug’ functions.

These models create co-ordinate systems they can map any two words, or chunck of words to.

Think about street address versus longitude and latitude. 
