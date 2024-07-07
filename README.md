# Identify outliers in the top-5 leagues
Recruitment is one of the most important use cases for football analytics. One commonly used method is to identify similar players. Another side of the coin is to identify players that are the most different from other players (known as outliers).

In this repository we aim to identify players that are are outliers in the data, e.g. they have a high number of expected goals compared to other players. We are using isolation forests that seek to isolate the anomalies based on an ensemble of isolation trees.

mamba create --name outliers-football sqlite jupyterlab pandas seaborn pyarrow beautifulsoup4 requests scikit-learn lxml pillow openpyxl plotly_express shap fuzzywuzzy metaphone mplsoccer ipywidgets pip

pip install mplsoccer fuzzymatcher

conda activate outliers-football

pip install fuzzymatcher
