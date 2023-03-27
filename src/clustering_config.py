european_langs = ["hr",
	"cs",
	"da",
	"nl",
	"en",
	"et",
	"fi",
	"fr",
	"de",
	"el",
	"hu",
	"ga",
	"it",
	"lv",
	"lt",
	"mt",
	"pl",
	"pt",
	"ro",
	"sk",
	"sl",
	"es",
	"sv"]

data_config = {
"languages": ["en", "es"], 
"columns": ["claimReviewed", "summary"] # claimReviewed, summary, text
	}

cluster_config = {
"embedder": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", # symanto/sn-xlm-roberta-base-snli-mnli-anli-xnli
"reducer": "umap", # PCA, umap, etc
"clusterer": "affinity", # affinity, etc
	}

cluster_alg_config = {
	
	"hdbscan":{
				"min_cluster_size": 20
			  },
	
	"affinity":{
				'preference': -50,
				'damping': 0.5
				}
}
