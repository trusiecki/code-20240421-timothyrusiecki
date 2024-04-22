import pandas as pd
from typing import Callable

# helper function to simplify the syntax
def map_column(df:pd.DataFrame,col:str,x:Callable) -> None:
    df[col] = df[col].map(x)

# import the TSV into a dataframe, separating the columns
location = r'C:\interview_analysis_molecule_x_10mg_v1.tsv'
df = pd.read_csv(filepath_or_buffer=location, sep='\t')

# define lambdas to apply to columns, 1. to validate numbers, 2. to split multiple data entries into lists
desci_func = lambda x: format(float(x),'8f')
split_func = lambda x: x.split('|')

# specify columns to apply functions to
# in a larger dataset, this might be improved by looping through column names and identifying what needs work from the column name
explodable = ['participants', 'participants_price']
numerical = ['quantity_annual', 'quantity_total', 'maximum_price_allowed','participants_price','winner_price','second_place_price']

# expand the datapoints with multiple values into lists
for item in explodable:
    map_column(df,item,split_func)

# create a new dataframe with one row per participant in an experiment
# certain datapoints which are orders of magnitude different from the other ones would likely be dropped after a sense check with a stakeholder/SME
df_2 = df.explode(explodable)

# validate that numbers are read properly
for item in numerical:
    map_column(df_2,item,desci_func)

# Create a winner_price check by comparing it with the actual smallest participants_price within each contract_id
df_2['price_min'] = df_2.groupby('contract_id')['participants_price'].min().map(desci_func).shift(-1)
df_2['price_check'] = df_2['price_min'] == df_2['winner_price']
df_2.drop('price_min')

# CONCLUSION
# winner_price has been validated (the Boolean in price_check) to be correct to the extent to which I have been able to understand the data without exposure to more data/a stakeholder/an SME.
# The first row was validated manually, not using the process above, as the time to debug the process above to cover it was so much greater than the time to do a visual check in this context.

# ROADMAP
# Conversations with stakeholders would establish how the initial dataset is created and whether first-line-of-defence data validation is applied at a previous stage (as some columns would suggest).
# I would also find out the projected volume of data flowing in, and structure development around that.
# A database of company's products could be used to enhance validation of impossible combinations.
# I would initially focus on implementing validation which a simpler algorithm cannot cover, then scale to improve efficiency.

# FURTHER POSSIBLE VALIDATIONS
# While this particular dataset did not present such problems, and its size did not warrant further automations at this stage, in a larger amount of data, I would check things such as whether:
# second_place_outcome is not blank when second_place and second_place_price are not, and vice versa
# number of participants matches the number of participants and participant prices listed
# quantity numbers make sense (after a conversation with an SME to establish what they represent)
# atc shows whatever it is meant to show and only when necessary
# dates validate for the company's founding date and the present/end of the reporting period for contract starts, largest contract length in the future for contract ends, etc.

# GOING FORWARD/FOR A LARGER DATASET
# I would still perform a visual sense check regardless of the size of the output to manage unknown unknowns.
# I would negotiate suggestive column names as to be able to automatically recognise columns needing validation.
# Depending on the needs of the model etc., I would establish the need for explicit values where there are blanks, such as in second_place_outcome.
# A rewrite in Polars might be necessary for efficiency once the volume of data becomes significant.
