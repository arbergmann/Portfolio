# Portfolio of Data Science Work
Portfolio of non-proprietary microprojects and code snippets, including academic, self-study, and hobby projects.

Updated regularly with new and intriguing projects that I feel exhibit unique skills, libraries, or concepts.

## Projects
Academic projects for Milestones I & II and Capstone for the University of Michigan's Master of Applied Data Science program.

* [Value Stock Screener](https://arbergmann.github.io/value_stock_screener/): <i>An early project in the program, designed to create a data pipeline that scraped for the current S&P 500 ticker list, pulled data from Quandl (now Nasdaq) and Yahoo! Finance, and then through modular code filters for value stocks and returns a dataframe report of possible options and corresponding investing metrics.</i>
* <a href="/assets/Milestone II Final Report.pdf" target="_blank">Stock Price Prediciton and Clustering</a>: <i>Analysis of FTSE 100 stocks, leveraging basic supervised and unsupervised methods to identify model performance against the benchmark, and identify trends and traits of certain potential trading categories.</i> 
* [Survey and Backtesting of Advanced Stock Price Forecasting Models](https://augurychris.github.io/financial_forecasting_analysis/): <i>An expansion on concepts of the previous project, developing an end-to-end machine learning pipeline for forecasting stock prices on a daily scale. The dataset contained the S&P 500 tickers (reduced), and a survey of linear and non-linear models. The models were trained, tested, and validated on a ensembles of fundamental, technical, and decomposed features, and run against a backtesting framework to detect actual alpha generation in practical trading applications as a form of cross-validation.</i>



## Proof of Concept Work
* [Pricing and Monetization Dashboard](https://github.com/arbergmann/revOps_dashboard): <i>A proof of concept project using mock data to practice modeling out different ways to design a rev ops/pricing dashboard. UX portion still under construction.</i>



## Data Manipulation and Processing
Code samples and exercises in Jupyter Notebooks exploring the utilizations of NLP, Time Series analysis, and data stream sampling.

* [N-Gram Language Models (Markov Models)](https://github.com/arbergmann/portfolio/blob/main/data_manipulation_and_processing/N-Gram%20Language%20Models%20(Markov%20Models).ipynb): <i>A collection of exercises and code exploring n-grams, Markov chains (including a Shakespearean Sonnet generator), and Markov Models for part-of-speech (POS) prediction tagging.  </i> (<b>Libraries:</b> <i>NLTK</i>)
* [Time Series Analysis (Basic)](https://github.com/arbergmann/portfolio/blob/main/data_manipulation_and_processing/Time%20Series%20Analysis%20(Basic).ipynb): <i>A collection of exercises and code exploring time series analysis through COVID-19 data. These exercises explore seasonal decomposition, trend analysis, weighted and exponential moving averages, similarity measures (Euclidian and Cosine), Dynamic Time Warping (DTW), Stationarity testing, AC, PAC, Single Time Series Forecasting (ARIMA), Multiple Time Series Forecasting (VAR), and Granger Causality.  </i> (<b>Libraries:</b> <i>NumPy, Pandas, Statsmodels, Sklearn, matplotlib</i>)
* [Data Stream Sampling](https://github.com/arbergmann/portfolio/blob/main/data_manipulation_and_processing/Data%20Stream%20Sampling.ipynb): <i>Leveraging a Twitter dataset to explore data sampling techniques including Random and Reservoir Sampling, counting methods including Bloom filters and Lossy counters. </i> (<b>Libraries:</b> <i>NumPy, Pandas, collections, matplotlib</i>)


## Big Data Processing
Code samples and exercises in Jupyter Notebooks exploring big data processing through MRJob and PySpark - leveraging RDDs, UDFs, and SQL.

* [MRJob](https://github.com/arbergmann/portfolio/blob/main/big_data_exercises/mrjob.ipynb): <i>Using Map Reduce to mine information from large Project Gutenberg text files. An intro to split-apply-combine techniques before moving into faster Spark-based applications.  </i> (<b>Libraries:</b> <i>MRJob, re, itertools, syllapy</i>)
* [Spark + NLTK (RDD Focus)](https://github.com/arbergmann/portfolio/blob/main/big_data_exercises/Spark%20%2B%20NLTK%20(RDD%20Focus).ipynb): <i>Exercises to expand familiarity with PySpark, work on basics of Spark RDD API, and practice applications of NLTK. Basic introduction to NLP by parsing a large text corpus for part-of-speech tagging and converts a Python-based script to a PySpark-based approach.  </i> (<b>Libraries:</b> <i>NLTK, re, PySpark</i>)
* [Spark + UDF](https://github.com/arbergmann/portfolio/blob/main/big_data_exercises/Spark%20%2B%20UDF.ipynb): <i>Expand familiarity with PySpark and work on basics of User-Defined Functions on a Yelp Academic 15gb dataset. Aim of exercises are to mine dataset for sentiment analysis, traits of useful/positive reviews, etc.  </i> (<b>Libraries:</b> <i>PySpark</i>)
* [Spark + SQL](https://github.com/arbergmann/portfolio/blob/main/big_data_exercises/Spark%20%2B%20SQL.ipynb): <i>Expand familiarity with PySpark and work on basics of User-Defined Functions on a Yelp Academic 15gb dataset. Aim of analysis is to mine dataset for various inclusion and usefulness statistics within the reviews.  </i> (<b>Libraries:</b> <i>PySpark</i>)


## Visualization
Code samples and exercises in Jupyter Notebooks primarily using the Altair visualization library. Due to known rendering issues within GitHub and in `html` format, PDFs are also available, but lack the ideal formatting and interactivity that is provided in the Jupyter notebooks. (<b>Libraries:</b> <i>NumPy, Pandas, Altair, networkx, nx_altair, matplotlib, Seaborn, squarify, Sklearn, SciPy, json</i>)

* [Crash Reports](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_crash_reports.ipynb): <i>An exploration of crash reports in Chicago along specific thoroughfares.  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_crash_reports.pdf))
* [Bechdel Test](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_bechdel.ipynb):  <i>A look at 50 movies against new ways of measuring and visualizing gender imabalnce.  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_bechdel.pdf))
* [Star Wars](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_star_wars.ipynb):  <i>A look at Star Wars fan preferences of movies and characters, and (obviously) who shot first (it's Han Solo and you know it).  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_star_wars.pdf))
* [Mayweather-McGregor](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_mayweather_mcgregor.ipynb):  <i>The Mayweather-McGregor fight as told by Twitter/Emojis.  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_mayweather_mcgregor.pdf))
* [Bob Ross](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_bob_ross.ipynb):  <i>Bob Ross painting analysis, specifically feature inclusion in each of his paintings. (<b>Note:</b> Interactive notebook)  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_bob_ross.pdf))
* [Marvel vs DC](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_marvel_vs_dc.ipynb):  <i>Comparison of Marvel vs DC characters, specifically analytics of new character introduction. (<b>Note:</b> Interactive notebook)  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_marvel_vs_dc.pdf))
* [Simpsons](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/altair_simpsons.ipynb):  <i>A visual look at quotable convsations in The Simpsons, leveraging network analysis in Altair. (<b>Note:</b> Interactive notebook)  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/altair_simpsons.pdf))
* [Plotly Demo Walkthrough](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/plotly_demo_walkthrough.ipynb):  <i>A small project writeup that required creating a tutorial for a visualization library (Plotly). (<b>Note:</b> Interactive notebook)  </i> ([PDF](https://github.com/arbergmann/portfolio/blob/main/visualization_exercises/pdfs/plotly_demo_walkthrough.pdf))
