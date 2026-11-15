# Dataset acquisition

Do not download data through this repository or commit it. After independently
reviewing license/terms, acquire PaySim from its [Kaggle dataset page](https://www.kaggle.com/datasets/ealaxi/paysim1) and IEEE-CIS from its [competition page](https://www.kaggle.com/competitions/ieee-fraud-detection). Store files under ignored `datasets/raw/` or an external secure path.

For each acquisition, calculate SHA-256, populate the matching template in
`datasets/metadata/`, record the source-specific identity rule, and profile
columns before enabling adapters. Do not merge their domains or fabricate stats.
