import sys, os
import warnings
sys.path.append('../build/')

import kuzu

db_path = './ml-small'
# if os.path.exists(db_path):
    # remove the old database
    # os.system('rm -rf ' + db_path)
    
def load_data(connection):
    print('loading data...')
    connection.execute('CREATE NODE TABLE movie (movieId INT64, title STRING, genres STRING, PRIMARY KEY (movieId))')
    connection.execute('CREATE NODE TABLE user (userId INT64, PRIMARY KEY (userId))')
    connection.execute('CREATE REL TABLE rating (FROM user TO movie, rating DOUBLE, timestamp INT64)')

    connection.execute('COPY movie FROM "./movieLen-small/movies.csv" (HEADER=TRUE)')
    connection.execute('COPY user FROM "./movieLen-small/users.csv" (HEADER=TRUE)')
    connection.execute('COPY rating FROM "./movieLen-small/ratings.csv" (HEADER=TRUE)')

db = kuzu.Database(db_path)
conn = kuzu.Connection(db)
# load_data(conn)

res = conn.execute('MATCH (u:user)-[r:rating]->(m:movie) RETURN u, r, m')
with warnings.catch_warnings(record=True) as ws:
    torch_geometric_data, pos_to_idx, unconverted_properties = res.get_as_torch_geometric()

import pandas as pd
import torch

df = pd.DataFrame.from_dict(unconverted_properties['movie'])

# encode genres
genres = df['genres'].str.get_dummies('|').values
genres = torch.from_numpy(genres).to(torch.float)

# encode title
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
with torch.no_grad():
    emb = model.encode(df['title'].values, show_progress_bar=True, convert_to_tensor=True).cpu()

torch_geometric_data['movie'].x = torch.cat([emb, genres], dim=1)
del torch_geometric_data['movie'].movieId
# TODO: add edge labels

# Add user node features for message passing:
torch_geometric_data['user'].x = torch.eye(torch_geometric_data['user'].userId.shape[0])
del torch_geometric_data['user'].userId

import torch_geometric.transforms as T
# Add a reverse ('movie', 'rev_rates', 'user') relation for message passing:
torch_geometric_data = T.ToUndirected()(torch_geometric_data)

print(torch_geometric_data)

