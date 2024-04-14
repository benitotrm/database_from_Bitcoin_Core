#%% GRAPH BASE BUILD
### GRAPH BASE BUILD

# from_address 
# Output (txid = 100, n = 1) -> Input (vin_txid = 100, vout = 1)
# Output (txid = 100, n = 2) -> Input (vin_txid = 100, vout = 2)

# to_address
# Input (txid = 402, vin_txid = 100, vout = 1) -> Output (txid = 402, n = 1) 
# Input (txid = 402, vin_txid = 100, vout = 2) -> Output (txid = 402, n = 2) 

import pandas as pd
import dask.dataframe as dd

# Load data
blocks = dd.read_parquet('_blocks.parquet')
transactions = dd.read_parquet('_transactions_parquets/*.parquet')
transactions_with_blocks = transactions.merge(blocks[['block_hash', 'time']], on='block_hash', how='left')
inputs = dd.read_parquet('_vin_parquets/*.parquet')
outputs = dd.read_parquet('_vout_parquets/*.parquet')
address_behavior = dd.read_csv('address_behavior_dataset.csv')

# Filters 
address_behavior_unique = address_behavior.drop_duplicates(subset=['address'])
outputs = outputs.assign(address=outputs['addresses'].map(lambda x: x[0] if x else None))
outputs_filtered = outputs.merge(address_behavior_unique[['address']], on='address', how='inner')

# Join to create from_address
inputs_with_address = inputs.merge(outputs_filtered[['txid', 'n', 'address']], left_on=['vin_txid', 'vout'], right_on=['txid', 'n'], how='inner')
inputs_with_address = inputs_with_address.rename(columns={'txid_x': 'txid', 'address': 'from_address'})
inputs_with_address = inputs_with_address.drop(['txid_y', 'n'], axis=1)

# Join to create to_address
transactions_with_addresses = inputs_with_address.merge(outputs_filtered, left_on=['txid', 'vout'], right_on=['txid', 'n'], how='inner')
transactions_with_addresses = transactions_with_addresses.rename(columns={'address': 'to_address'})

# Add labels
final_df = transactions_with_addresses.merge(address_behavior.rename(columns={'address': 'from_address', 'label': 'label_x'}), on='from_address', how='inner')
final_df = final_df.merge(address_behavior.rename(columns={'address': 'to_address', 'label': 'label_y'}), on='to_address', how='inner')

# Convert 'time' column in transactions_with_blocks to datetime and merge with final_df to add date
transactions_with_blocks['date'] = dd.to_datetime(transactions_with_blocks['time'], unit='s')
final_df = final_df.merge(transactions_with_blocks[['txid', 'date']], on='txid', how='left')

# Clean final dataframe
final_df = final_df[['txid', 'from_address', 'label_x', 'to_address', 'label_y', 'value', 'date']]
final_df['value'] = final_df['value'].astype('float32')
final_df = final_df.dropna()
final_df = final_df.drop_duplicates()

# Save the final dataframe
print(final_df.dtypes)
final_df.to_parquet('final_dataset_for_gnn', write_index=False)

#%% GRAPH BASE DQ
### GRAPH BASE DQ
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option('display.float_format', '{:.0f}'.format)

# Load the processed data from the Parquet files
df = dd.read_parquet('final_dataset_for_gnn')

# Check data types
print("\nData Types:")
print(df.dtypes)

# Display a sample of the data
print("Sample of the Data:")
print(df.head())

# Data Description and Quality Checks
print("\nData Description:")
descriptive_stats = df.describe().compute() 
descriptive_stats['value'] = descriptive_stats['value'].apply(lambda x: f'{x:.8f}' if pd.notnull(x) else x)
print(descriptive_stats)

# Checking for missing values
print("\nMissing Values in Each Column:")
print(df.isnull().sum().compute())

# Compute distinct values and counts for label_x
label_x_counts = df.groupby('label_x').size().compute()

# Compute distinct values and counts for label_y
label_y_counts = df.groupby('label_y').size().compute()

# Convert Dask Series to Pandas DataFrame for easier plotting
label_x_counts_df = label_x_counts.to_frame("counts").reset_index()
label_y_counts_df = label_y_counts.to_frame("counts").reset_index()

# Plotting for label_x
plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
plt.bar(label_x_counts_df['label_x'], label_x_counts_df['counts'], color='blue')
plt.xlabel('Label X')
plt.ylabel('Counts')
plt.title('Counts of Distinct Values for label_x')
plt.xticks(rotation=45)  # Rotate labels to make them readable
plt.show()

# Plotting for label_y
plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
plt.bar(label_y_counts_df['label_y'], label_y_counts_df['counts'], color='green')
plt.xlabel('Label Y')
plt.ylabel('Counts')
plt.title('Counts of Distinct Values for label_y')
plt.xticks(rotation=45)  # Rotate labels to make them readable
plt.show()
#%% Decay rate
### Decay rate
import numpy as np
import matplotlib.pyplot as plt

# Define the timespan in days
days = np.arange(0, 365, 1)  # Example: 1 year

# Define various decay rates to compare
decay_rates = [0.01, 0.05, 0.1, 0.2]

# Calculate decay factors for each decay rate over the timespan
for decay_rate in decay_rates:
    decay_factors = np.exp(-decay_rate * days)
    plt.plot(days, decay_factors, label=f'Decay rate = {decay_rate}')

# Configure plot
plt.title('Decay Weight of Transactions Over Time')
plt.xlabel('Days Since Last Transaction')
plt.ylabel('Decay Factor')
plt.legend()
plt.grid(True)
plt.show()


#%% GRAPH BUILD
### GRAPH BUILD
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
import numpy as np
import torch
from torch_geometric.data import Data
from sklearn.preprocessing import LabelEncoder

year = 2013

# Adjust these parameters as needed
start_date = f'{year-1}-12-21'
end_date = f'{year}-12-21'
decay_rate = 0.01

# Directly load data into Dask DataFrame
ddf = dd.read_parquet('final_dataset_for_gnn', engine='pyarrow')

# Ensure 'date' is in datetime format and filter by date range
ddf['date'] = dd.to_datetime(ddf['date'])
final_ddf = ddf[(ddf['date'] >= start_date) & (ddf['date'] <= end_date)]
final_ddf = final_ddf.persist()

# Calculate decay factors in parallel
max_date = final_ddf['date'].max()
final_ddf['days_since_last'] = (max_date - final_ddf['date']).dt.days
final_ddf['decay_factor'] = np.exp(-decay_rate * final_ddf['days_since_last'])
agg_decay_factors = final_ddf.groupby(['from_address', 'to_address'])['decay_factor'].sum().reset_index().compute()

# Perform aggregations in parallel
with ProgressBar():
    incoming_values_ddf = final_ddf.groupby('to_address')['value'].sum()
    outgoing_values_ddf = final_ddf.groupby('from_address')['value'].sum()
    # Convert directly to Pandas for further processing
    incoming_values = incoming_values_ddf.compute().reset_index()
    outgoing_values = outgoing_values_ddf.compute().reset_index()

# Convert Dask DataFrame to Pandas for further processing
final_df = final_ddf.compute()

# Encode labels
label_encoder = LabelEncoder()
final_df['label_x'] = label_encoder.fit_transform(final_df['label_x'].astype(str))
final_df['label_y'] = label_encoder.fit_transform(final_df['label_y'].astype(str))

label_mapping = {i: label for i, label in enumerate(label_encoder.classes_)}

# Index addresses 
address_labels_from = final_df[['from_address', 'label_x']].rename(columns={'from_address': 'address', 'label_x': 'label'})
address_labels_to = final_df[['to_address', 'label_y']].rename(columns={'to_address': 'address', 'label_y': 'label'})
address_label_df = pd.concat([address_labels_from, address_labels_to]).drop_duplicates().reset_index(drop=True)
address_label_map = pd.Series(address_label_df.label.values,index=address_label_df.address).to_dict()
all_addresses = address_label_df['address'].unique()

address_to_idx = {address: idx for idx, address in enumerate(all_addresses)}
idx_to_address = {idx: address for address, idx in address_to_idx.items()}

all_addresses_df = pd.DataFrame(all_addresses, columns=['address'])
all_addresses_df = all_addresses_df.merge(incoming_values, how='left', left_on='address', right_on='to_address')
all_addresses_df = all_addresses_df.merge(outgoing_values, how='left', left_on='address', right_on='from_address')

# Nodes tensors
node_labels = [address_label_map[address] for address in all_addresses]
node_labels_tensor = torch.tensor(node_labels, dtype=torch.long)

node_features_array = np.array(all_addresses_df[['value_x', 'value_y']].fillna(0))
node_features_tensor = torch.tensor(node_features_array, dtype=torch.float)

# Edge tensor
edges_df = final_df[['from_address', 'to_address']].copy()
edge_index_np = np.array([edges_df['from_address'].map(address_to_idx).values, 
                          edges_df['to_address'].map(address_to_idx).values])
edge_index = torch.tensor(edge_index_np, dtype=torch.long)

# Edge weights tensor
agg_decay_factors['edge_key'] = agg_decay_factors['from_address'] + '_' + agg_decay_factors['to_address']
decay_factor_map = pd.Series(agg_decay_factors['decay_factor'].values, index=agg_decay_factors['edge_key']).to_dict()

edges_df['edge_key'] = edges_df['from_address'] + '_' + edges_df['to_address']
edges_df['decay_factor'] = edges_df['edge_key'].map(decay_factor_map)
edge_weights_tensor = torch.tensor(edges_df['decay_factor'].values, dtype=torch.float)

# Create the graph
graph_data = Data(x=node_features_tensor, edge_index=edge_index, edge_attr=edge_weights_tensor, y=node_labels_tensor)

torch.save({
    'graph_data': graph_data, 
    'idx_to_address': idx_to_address, 
    'address_to_idx': address_to_idx,
    'label_mapping': label_mapping 
}, f'graph_data_year_with_mapping ({year}).pt')

# torch.save({'graph_data': graph_data, 'idx_to_address': idx_to_address}, 'graph_data_full_with_mapping.pt')
print(graph_data)

#%% GRAPH EXPLORE (Reestart point)
### GRAPH EXPLORE (Reestart point)
import torch
from torch_geometric.data import Data
from torch_geometric.utils import degree, to_networkx
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

year = 2013

# Load the graph data
graph_data = torch.load(f'graph_data_year_with_mapping ({year}).pt')
# graph_data = torch.load('graph_data_full_with_mapping.pt')

# Access the attributes using keys
edge_index = graph_data['graph_data'].edge_index
node_features = graph_data['graph_data'].x
node_labels = graph_data['graph_data'].y
edge_weights = graph_data['graph_data'].edge_attr

# Print the number of nodes and edges in the graph
print(f"Number of nodes: {graph_data['graph_data'].num_nodes}")
print(f"Number of edges: {graph_data['graph_data'].num_edges}")

# Print node features, labels, and edge index info
print(f"Node features (min, max): {node_features.min()}, {node_features.max()}")
print(f"Node labels: {node_labels.unique()}")
print(f"Edge index (min, max): {edge_index.min()}, {edge_index.max()}")

# Print the degree of nodes, considering it's a directed graph
node_degrees = degree(edge_index[0])  # Use 0 for source nodes, 1 for target nodes
print(f"Degree of nodes (min, max): {node_degrees.min()}, {node_degrees.max()}")

# Print the first 5 node features, labels, and edges
print(f"First 5 node features:\n{node_features[:5]}")
print(f"First 5 node labels:\n{node_labels[:5]}")
print(f"First 5 edges:\n{edge_index[:, :5]}")
print(f"First 5 edge weights:\n{edge_weights[:5]}")  # Print first 5 edge weights

# NaN or Inf checks
if torch.isnan(node_features).any() or torch.isinf(node_features).any():
    print("NaN or Inf in node features")
if torch.isnan(node_labels).any() or torch.isinf(node_labels).any():
    print("NaN or Inf in node labels")

#%% GRAPH VISUAL EXPLORE 
### GRAPH VISUAL EXPLORE 
import torch
from torch_geometric.data import Data
from torch_geometric.utils import degree, to_networkx
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

# Load the graph data
graph_data = torch.load('graph_data_year_with_mapping.pt')
# graph_data = torch.load('graph_data_full_with_mapping.pt')

# Access the attributes using keys
edge_index = graph_data['graph_data'].edge_index
node_features = graph_data['graph_data'].x
node_labels = graph_data['graph_data'].y
edge_weights = graph_data['graph_data'].edge_attr

# Node label count visualization
unique_labels, counts = node_labels.unique(return_counts=True)
plt.figure(figsize=(10, 5))
plt.bar(unique_labels.numpy(), counts.numpy())
plt.xlabel('Label')
plt.ylabel('Count')
plt.title('Node Label Distribution')
plt.show()

# Node degree distribution
degrees = degree(edge_index[0]).numpy()  # Assuming edge_index[0] contains source nodes
plt.figure(figsize=(10, 5))
plt.hist(degrees, bins=np.logspace(np.log10(1), np.log10(degrees.max()), 50))
plt.gca().set_xscale("log")
plt.gca().set_yscale("log")
plt.xlabel('Degree')
plt.ylabel('Frequency')
plt.title('Node Degree Distribution')
plt.show()

# Visual comparison of nodes and edges
objects = ('Nodes', 'Edges')
y_pos = np.arange(len(objects))
performance = [graph_data['graph_data'].num_nodes, graph_data['graph_data'].num_edges]

plt.figure(figsize=(10, 5))
plt.bar(y_pos, performance, align='center', alpha=0.5)
plt.xticks(y_pos, objects)
plt.ylabel('Count')
plt.title('Number of Nodes and Edges')

plt.show()

# Edge weight distribution
plt.figure(figsize=(10, 5))
plt.hist(edge_weights.numpy(), bins=50, log=True)
plt.xlabel('Edge Weight')
plt.ylabel('Frequency')
plt.title('Edge Weight Distribution')
plt.show()

plt.figure(figsize=(20, 10))

# Plot 1: Node Label Distribution
plt.subplot(2, 2, 1)
plt.bar(unique_labels.numpy(), counts.numpy())
plt.xlabel('Label')
plt.ylabel('Count')
plt.title('Node Label Distribution')

# Plot 2: Node Degree Distribution
plt.subplot(2, 2, 2)
plt.hist(degrees, bins=np.logspace(np.log10(1), np.log10(degrees.max()), 50))
plt.gca().set_xscale("log")
plt.gca().set_yscale("log")
plt.xlabel('Degree')
plt.ylabel('Frequency')
plt.title('Node Degree Distribution')

# Plot 3: Number of Nodes and Edges
plt.subplot(2, 2, 3)
plt.bar(y_pos, performance, align='center', alpha=0.5)
plt.xticks(y_pos, objects)
plt.ylabel('Count')
plt.title('Number of Nodes and Edges')

# Plot 4: Edge Weight Distribution
plt.subplot(2, 2, 4)
plt.hist(edge_weights.numpy(), bins=50, log=True)
plt.xlabel('Edge Weight')
plt.ylabel('Frequency')
plt.title('Edge Weight Distribution')

plt.tight_layout()
plt.show()


#%% GRAPH MEASSURES (Top addresses)
### GRAPH MEASSURES (Top addresses)
import torch
from torch_geometric.utils import to_networkx
import networkx as nx

year = 2022

# Loading graph data and idx_to_address mapping
loaded_data = torch.load(f'graph_data_year_with_mapping ({year}).pt')
graph_data = loaded_data['graph_data']
idx_to_address = loaded_data['idx_to_address']

# Convert to NetworkX graph (ensure to pass edge weights)
G = to_networkx(graph_data, to_undirected=False, edge_attrs=['edge_attr'])

# Calculate centrality measures for directed graphs
in_degree_centrality = nx.in_degree_centrality(G)
out_degree_centrality = nx.out_degree_centrality(G)
betweenness_centrality = nx.betweenness_centrality(G, weight='weight')
closeness_centrality = nx.closeness_centrality(G) 

# For average path length, we calculate it for the largest strongly connected component
largest_cc = max(nx.strongly_connected_components(G), key=len)
G_sub = G.subgraph(largest_cc)
try:
    avg_path_length = nx.average_shortest_path_length(G_sub, weight='weight')
except nx.NetworkXError:
    avg_path_length = float('inf')  # If the graph is not connected, path length is infinite

# Additional Metrics
num_nodes = G.number_of_nodes()
num_edges = G.number_of_edges()
avg_in_degree = sum(dict(G.in_degree()).values()) / float(num_nodes)
avg_out_degree = sum(dict(G.out_degree()).values()) / float(num_nodes)
density = nx.density(G)

# Convert top 5 nodes from each centrality measure from indices to addresses
top_5_in_degree = sorted(in_degree_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
top_5_out_degree = sorted(out_degree_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
top_5_betweenness = sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:5]

# Function to convert indices to addresses
def indices_to_addresses(top_5, idx_to_address):
    return [(idx_to_address[idx], score) for idx, score in top_5]

# Print top 5 nodes with addresses
print("Top 5 In-Degree Centrality Nodes:", indices_to_addresses(top_5_in_degree, idx_to_address))
print("Top 5 Out-Degree Centrality Nodes:", indices_to_addresses(top_5_out_degree, idx_to_address))
print("Top 5 Betweenness Centrality Nodes:", indices_to_addresses(top_5_betweenness, idx_to_address))

# Print additional metrics
print("\nAdditional Metrics:")
print("Number of Nodes:", num_nodes)
print("Number of Edges:", num_edges)
print("Average In-Degree:", avg_in_degree)
print("Average Out-Degree:", avg_out_degree)
print("Density:", density)
print("Average Path Length (largest strongly connected component):", avg_path_length)
#%% GRAPH MEASSURES (General)
### GRAPH MEASSURES (General)
import torch
from torch_geometric.utils import to_networkx
import networkx as nx

year = 2013

# Loading graph data and idx_to_address mapping
loaded_data = torch.load(f'graph_data_year_with_mapping ({year}).pt')
graph_data = loaded_data['graph_data']
idx_to_address = loaded_data['idx_to_address']
node_features_tensor = graph_data.x

# Convert to NetworkX graph (ensure to pass edge weights)
G = to_networkx(graph_data, to_undirected=False, edge_attrs=['edge_attr'])

# Calculate average incoming and outgoing values
avg_incoming_value = torch.mean(node_features_tensor[:, 0]).item()
avg_outgoing_value = torch.mean(node_features_tensor[:, 1]).item()

# Calculate centrality measures for directed graphs
in_degree_centrality = nx.in_degree_centrality(G)
out_degree_centrality = nx.out_degree_centrality(G)

# For average path length, we calculate it for the largest strongly connected component
largest_cc = max(nx.strongly_connected_components(G), key=len)
G_sub = G.subgraph(largest_cc)
try:
    avg_path_length = nx.average_shortest_path_length(G_sub, weight='weight')
except nx.NetworkXError:
    avg_path_length = float('inf')  # If the graph is not connected, path length is infinite

# Additional Metrics
num_nodes = G.number_of_nodes()
num_edges = G.number_of_edges()
avg_in_degree = sum(dict(G.in_degree()).values()) / float(num_nodes)
avg_out_degree = sum(dict(G.out_degree()).values()) / float(num_nodes)
density = nx.density(G)

# Print metrics
print("\nMetrics:")
print("Number of Nodes:", num_nodes)
print("Number of Edges:", num_edges)
print("Average In-Degree:", avg_in_degree)
print("Average Out-Degree:", avg_out_degree)
print("Density:", density)
print("Average Path Length (largest strongly connected component):", avg_path_length)
print("Average Incoming Value:", avg_incoming_value)
print("Average Outgoing Value:", avg_outgoing_value)

#%% GRAPH VISUALIZATION
### GRAPH VISUALIZATION
import matplotlib.pyplot as plt

# Number of top nodes to select for visualization
top_n = 100

# Select top nodes based on a centrality measure
# For a directed graph, using in-degree centrality as an example
top_nodes = sorted(in_degree_centrality, key=in_degree_centrality.get, reverse=True)[:top_n]

# Create a subgraph with these top nodes
subG = G.subgraph(top_nodes)

# Generate positions for the nodes using a layout algorithm
pos = nx.spring_layout(subG)

# Visualizing the subgraph of top nodes
plt.figure(figsize=(12, 12))
nx.draw(subG, pos, with_labels=True, node_color='skyblue', edge_color='gray', node_size=50, font_size=8)
plt.title('Subgraph Visualization of Top Nodes by In-Degree Centrality')
plt.show()

#%% MODEL
### MODEL
import optuna
import torch
import torch.nn.functional as F
from torch_geometric.nn import GATConv
import torch.optim as optim
from sklearn.metrics import precision_score, recall_score, f1_score
import matplotlib.pyplot as plt

# Get data
graph_data = graph_data['graph_data']
num_classes = graph_data.y.max().item() + 1
num_features = graph_data.num_node_features

# Set device for training (GPU or CPU)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f'Using device: {device}')

# Define the GNN model class
class SimpleGAT(torch.nn.Module):
    def __init__(self, num_features, num_classes, num_hidden_units=16, dropout_rate=0.5):
        super(SimpleGAT, self).__init__()
        self.conv1 = GATConv(num_features, num_hidden_units)
        self.conv2 = GATConv(num_hidden_units, num_classes)
        self.dropout_rate = dropout_rate

    def forward(self, data, return_probs=False):
        x, edge_index, edge_weight = data.x, data.edge_index, data.edge_attr
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=self.dropout_rate, training=self.training)
        x = self.conv2(x, edge_index)

        if return_probs:
            return F.softmax(x, dim=1)
        else:
            return F.log_softmax(x, dim=1)

# Define the Optuna objective function
def objective(trial):
    num_hidden_units = trial.suggest_categorical('num_hidden_units', [8, 16, 32, 64])
    dropout_rate = trial.suggest_float('dropout_rate', 0.1, 0.5)
    lr = trial.suggest_float('lr', 1e-5, 1e-1, log=True)
    weight_decay = trial.suggest_float('weight_decay', 1e-5, 1e-3, log=True)

    model = SimpleGAT(num_features=num_features, num_classes=num_classes,
                      num_hidden_units=num_hidden_units,
                      dropout_rate=dropout_rate).to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    
    for epoch in range(500):
        model.train()
        optimizer.zero_grad()
        out = model(graph_data)
        loss = F.nll_loss(out[train_indices], graph_data.y[train_indices])
        loss.backward()
        optimizer.step()
        
    model.eval()
    with torch.no_grad():
        pred = model(graph_data)[validation_indices].max(1)[1]
        accuracy = pred.eq(graph_data.y[validation_indices]).sum().item() / validation_indices.size(0)
    return accuracy

# Optimize hyperparameters
def optimize_hyperparameters():
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=100)
    return study.best_trial.params

def split(graph_data, train_ratio=0.7, validation_ratio=0.1, test_ratio=0.2):
    num_nodes = graph_data.num_nodes
    shuffled_indices = torch.randperm(num_nodes)
    
    train_end = int(train_ratio * num_nodes)
    validation_end = train_end + int(validation_ratio * num_nodes)

    train_indices = shuffled_indices[:train_end]
    validation_indices = shuffled_indices[train_end:validation_end]
    test_indices = shuffled_indices[validation_end:]
    
    return train_indices, validation_indices, test_indices

# Move graph data to the same device as the model
graph_data = graph_data.to(device)

# Split data
train_indices, validation_indices, test_indices = split(graph_data)
train_indices = train_indices.to(device)
validation_indices = validation_indices.to(device)
test_indices = test_indices.to(device)

# Optimize hyperparameters
best_params = optimize_hyperparameters()
print(best_params)

# Use best hyperparameters from Optuna to re-instantiate the model and its optimizer
simple_model = SimpleGAT(num_features=num_features, num_classes=num_classes).to(device)
optimizer = optim.Adam(simple_model.parameters(), lr=best_params['lr'], weight_decay=best_params['weight_decay'])

# Training function
def train():
    simple_model.train()
    optimizer.zero_grad()
    out = simple_model(graph_data)
    train_labels = graph_data.y[train_indices]
    train_out = out[train_indices]
    loss = F.nll_loss(train_out, train_labels)
    
    if torch.isnan(loss) or torch.isinf(loss):
        print("Loss is NaN or Inf")
        return

    loss.backward(retain_graph=True) 
    optimizer.step()
    return loss.item()

# Testing function
def test():
    simple_model.eval()
    correct = 0
    total = 0
    probabilities = []

    with torch.no_grad():
        out = simple_model(graph_data, return_probs=True)  
        test_labels = graph_data.y[test_indices]
        test_out = out[test_indices]
        pred = test_out.argmax(dim=1)
        correct += pred.eq(test_labels).sum().item()
        total += test_labels.size(0)
        probabilities = test_out.max(dim=1)[0]  

    avg_max_prob = probabilities.mean().item()  
    return correct / total, avg_max_prob

# Variables to store metrics
train_losses = []
test_accuracies = []
precisions = []
recalls = []
f1_scores = []
avg_max_probabilities = []

# Function to get predictions and true labels
def get_predictions_and_labels(data, indices):
    simple_model.eval()
    with torch.no_grad():
        out = simple_model(data)
        pred = out.argmax(dim=1)[indices].cpu().numpy()
        true = data.y[indices].cpu().numpy()
    return pred, true

# Training loop
for epoch in range(500):
    train_loss = train()
    test_acc, avg_max_prob = test()
    avg_max_probabilities.append(avg_max_prob)
    pred, true = get_predictions_and_labels(graph_data, test_indices)
    precisions.append(precision_score(true, pred, average='weighted', zero_division=0))
    recalls.append(recall_score(true, pred, average='weighted', zero_division=0))
    f1_scores.append(f1_score(true, pred, average='weighted'))
    train_losses.append(train_loss)
    test_accuracies.append(test_acc)
    
    if (epoch + 1) % 100 == 0:
        print(f'Epoch: {epoch+1}, Train Loss: {train_loss:.4f}, Test Acc: {test_acc:.4f}, Precision: {precisions[-1]:.4f}, Recall: {recalls[-1]:.4f}, F1 Score: {f1_scores[-1]:.4f}')

# Plotting
plt.figure(figsize=(18, 6))

# Plot training loss and test accuracy
ax1 = plt.subplot(1, 3, 1)
ax2 = ax1.twinx() 
ax1.plot(train_losses, 'g-', label='Training Loss')
ax2.plot(test_accuracies, 'b-', label='Test Accuracy')
ax1.set_xlabel('Epochs')
ax1.set_ylabel('Training Loss', color='g')
ax2.set_ylabel('Test Accuracy', color='b')
ax1.tick_params(axis='y', colors='g')
ax2.tick_params(axis='y', colors='b')
ax1.set_title('Training Loss and Test Accuracy')
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

# Plot precision, recall, and F1 score
plt.subplot(1, 3, 2)
plt.plot(precisions, label='Precision')
plt.plot(recalls, label='Recall')
plt.plot(f1_scores, label='F1 Score')
plt.xlabel('Epochs')
plt.ylabel('Metrics Value')
plt.title('Precision, Recall, and F1 Score')
plt.legend()

# Plot average max probabilities
plt.subplot(1, 3, 3)
plt.plot(avg_max_probabilities, label='Avg Max Probability')
plt.xlabel('Epochs')
plt.ylabel('Average Max Probability')
plt.title('Average Max Probability over Epochs')
plt.legend()

plt.tight_layout()
plt.show()
# %% Address review
#### Address review

# Load the necessary objects from the saved file
loaded_data = torch.load('graph_data_year_with_mapping.pt')
graph_data = loaded_data['graph_data']
address_to_idx = loaded_data['address_to_idx']
label_mapping = loaded_data['label_mapping']  # Load the label mapping
node_labels_tensor = graph_data.y  # Assuming the labels are stored in graph_data.y

def get_labels_for_addresses(address_list, address_to_idx, node_labels_tensor, label_mapping):
    """
    Given a list of addresses, return their original labels using the label mapping.

    Parameters:
    - address_list: A list of addresses (strings).
    - address_to_idx: A dictionary mapping addresses to indices.
    - node_labels_tensor: A tensor containing encoded labels for each node/index.
    - label_mapping: A dictionary mapping encoded labels back to original labels.

    Returns:
    - A list of original labels corresponding to the input addresses.
    """
    idx_list = [address_to_idx.get(address) for address in address_list]
    encoded_labels = [node_labels_tensor[idx].item() for idx in idx_list if idx is not None]
    original_labels = [label_mapping[label] for label in encoded_labels]
    return original_labels

# Example usage
address_list = ['12cgpFdJViXbwHbhrA3TuW1EGnL25Zqc3P', '1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ']
labels = get_labels_for_addresses(address_list, address_to_idx, node_labels_tensor, label_mapping)

print("Original labels for the provided addresses:", labels)

# %%
