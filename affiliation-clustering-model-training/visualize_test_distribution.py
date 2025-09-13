import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_theme(style='whitegrid')

df = pd.read_csv('test_triplet_scores.csv')
df = df[['positive_score', 'negative_score']]

sns.kdeplot(data=df)
plt.title('Distribution of Triplet Scores')
plt.xlabel('Triplet Score')
plt.ylabel('Density')
plt.savefig('triplet_scores_distribution.png', dpi=300, bbox_inches='tight')
