# Scoring policy

Historical `isFraud` labels remain ground truth where supplied. Generated scores
and alerts are separate operational records; they must never overwrite labels.
