import pandas as pd 

ARCHIVE_DIR = ""
df = pd.read_csv(f'../archive/olist_order_reviews_dataset.csv')

# Group by and count occurrences
counts = df.groupby(['review_id', 'order_id']).size().reset_index(name='count')
# Filter for counts >= 2
result = counts[counts['count'] >= 2]

print(result.head(20))