import os
import pandas as pd
import hdbscan
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import numpy as np
from sklearn.decomposition import PCA
import umap
import argparse
import clustering_config as conf
import requests
import json

def compose_texts(df, columns = ['title', 'summary']):
	return ['. '.join(k) for k in df[columns].values]

class Embedder:
    def __init__(self, model_name_or_path):
        self.model = SentenceTransformer(model_name_or_path)
    def embed(self, docs):
        return self.model.encode(docs)

class Reducer:

	def __init__(self, method):

		self.method = method
		self.n_components = 10

		if self.method == 'pca':
			self.reducer = PCA(n_components=self.n_components)
		elif self.method == 'umap':
			self.reducer = umap.UMAP(n_components=self.n_components)
		else:
			raise ValueError('Invalid method: {}'.format(self.method))

	def fit_transform(self, X):
		return self.reducer.fit_transform(X)

class Clusterer:
	def __init__(self, method, cluster_config):
		self.method = method
		if self.method == 'affinity':
			self.clusterer = AffinityPropagation(**cluster_config)
		elif self.method == 'hdbscan':
			self.clusterer = hdbscan.HDBSCAN(**cluster_config)
		else:
			raise ValueError('Invalid method: {}'.format(self.method))
	def fit(self, X):
		self.clusterer.fit(X)
	def get_labels(self):
		return self.clusterer.labels_		

def make_outpath(configlist):
	outf = ""
	for c in configlist:
		for k,v in c.items():
			outf+= f"{k}={v}__"
	outf = outf.strip('__')
	outf += '.csv'
	return outf

def log_to_logzio(df):
    """
    Log DataFrame to Logz.io via HTTP bulk API
    """
    logzio_token = 'dNNtYgTWTDauRykeswpwpzQTrTAXsoVy'
    logzio_url = 'https://listener.logz.io:8071/?token={}'.format(logzio_token)
    logs = df.to_dict(orient='records')
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(logzio_url, data=json.dumps(logs), headers=headers)
    response.raise_for_status()

if __name__ == '__main__':

	parser = argparse.ArgumentParser()

	parser.add_argument('-i','--input-dataset',help='Claims file', required=True)	
	parser.add_argument('-o','--output-folder',help='Directory to store results', required=True)	

	# Parse the argument
	args = parser.parse_args()

	### PREPARE ###
	# embed 
	embedder = Embedder(conf.cluster_config['embedder'])
	print(f'Embedder: {embedder.__dict__}')
	# reduce
	reducer = Reducer(conf.cluster_config['reducer'])
	print(f'Reducer: {reducer.__dict__}')
	# cluster
	clusterer = Clusterer(conf.cluster_config['clusterer'], # get the clustering algorithm
						  # get the config for the clustering algorithm
						  conf.cluster_alg_config[conf.cluster_config['clusterer']])
	print(f'Clusterer: {clusterer.__dict__}')	

	### FILTER DATASET ###
	df = pd.read_json(args.input_dataset)
	# filter and prepare dataset
	print(f'Target langs: {conf.data_config["languages"]}')
	df = df[df.languageISO.isin(conf.data_config["languages"])]
	print(f'Dataset size: {df.shape[0]} rows')
	# make corpus from claim texts
	texts = compose_texts(df, columns = conf.data_config['columns'])
	### EMBED, REDUCE AND CLUSTER ###
	print('Embedding...')
	embs = tqdm(embedder.embed(texts))
	embs = np.array(list(embs))
	print('Reducing...')
	embs_reduced = reducer.fit_transform(embs)
	print('Clustering...')
	clusterer.fit(embs_reduced)
	labels = clusterer.get_labels()
	df['cluster_labels'] = labels

	outfile_format = os.path.join(args.output_folder,"{language}_{reducer}_{n_components}_{method}_{model}.csv")
	outfile = outfile_format.format(language='-'.join(conf.data_config["languages"]),
									reducer = conf.cluster_config['reducer'], 
									n_components = reducer.n_components,
									method=conf.cluster_config['clusterer'], 
									model=conf.cluster_config['embedder'].replace('/','-'))

	sorted_df = df.sort_values(by='cluster_labels', key=lambda x: x.map(x.value_counts()))
	sorted_df[conf.data_config["columns"]+["cluster_labels"]].reset_index().to_csv(outfile)